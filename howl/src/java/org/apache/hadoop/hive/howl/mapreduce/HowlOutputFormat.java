/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.howl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.common.ErrorType;
import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.common.HowlUtil;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;

/** The OutputFormat to use to write data to Howl. The key value is ignored and
 * and should be given as null. The value is the HowlRecord to write.*/
public class HowlOutputFormat extends OutputFormat<WritableComparable<?>, HowlRecord> {

    //The keys used to store info into the job Configuration.
    //If any new keys are added, the HowlStorer needs to be updated. The HowlStorer
    //updates the job configuration in the backend to insert these keys to avoid
    //having to call setOutput from the backend (which would cause a metastore call
    //from the map jobs)
    public static final String HOWL_KEY_OUTPUT_BASE = "mapreduce.lib.howloutput";
    public static final String HOWL_KEY_OUTPUT_INFO = HOWL_KEY_OUTPUT_BASE + ".info";
    public static final String HOWL_KEY_HIVE_CONF = HOWL_KEY_OUTPUT_BASE + ".hive.conf";

    /** The directory under which data is initially written for a non partitioned table */
    protected static final String TEMP_DIR_NAME = "_TEMP";

    private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    };

    /**
     * Set the info about the output to write for the Job. This queries the metadata server
     * to find the StorageDriver to use for the table.  Throws error if partition is already published.
     * @param job the job object
     * @param outputInfo the table output info
     * @throws IOException the exception in communicating with the metadata server
     */
    @SuppressWarnings("unchecked")
    public static void setOutput(Job job, HowlTableInfo outputInfo) throws IOException {
      HiveMetaStoreClient client = null;

      try {
        Configuration conf = job.getConfiguration();
        client = createHiveClient(outputInfo.getServerUri(), conf);
        Table table = client.getTable(outputInfo.getDatabaseName(), outputInfo.getTableName());

        if( outputInfo.getPartitionValues() == null ) {
          outputInfo.setPartitionValues(new HashMap<String, String>());
        } else {
          //Convert user specified map to have lower case key names
          Map<String, String> valueMap = new HashMap<String, String>();
          for(Map.Entry<String, String> entry : outputInfo.getPartitionValues().entrySet()) {
            valueMap.put(entry.getKey().toLowerCase(), entry.getValue());
          }

          outputInfo.setPartitionValues(valueMap);
        }

        //Handle duplicate publish
        handleDuplicatePublish(job, outputInfo, client, table);

        StorageDescriptor tblSD = table.getSd();
        HowlSchema tableSchema = HowlUtil.extractSchemaFromStorageDescriptor(tblSD);
        StorerInfo storerInfo = InitializeInput.extractStorerInfo(table.getParameters());

        List<String> partitionCols = new ArrayList<String>();
        for(FieldSchema schema : table.getPartitionKeys()) {
          partitionCols.add(schema.getName());
        }

        Class<? extends HowlOutputStorageDriver> driverClass =
          (Class<? extends HowlOutputStorageDriver>) Class.forName(storerInfo.getOutputSDClass());
        HowlOutputStorageDriver driver = driverClass.newInstance();

        String tblLocation = tblSD.getLocation();
        String location = driver.getOutputLocation(job,
            tblLocation, partitionCols,
            outputInfo.getPartitionValues());

        //Serialize the output info into the configuration
        OutputJobInfo jobInfo = new OutputJobInfo(outputInfo,
                tableSchema, tableSchema, storerInfo, location, table);
        conf.set(HOWL_KEY_OUTPUT_INFO, HowlUtil.serialize(jobInfo));

        Path tblPath = new Path(tblLocation);

        /*  Set the umask in conf such that files/dirs get created with table-dir
         * permissions. Following three assumptions are made:
         * 1. Actual files/dirs creation is done by RecordWriter of underlying
         * output format. It is assumed that they use default permissions while creation.
         * 2. Default Permissions = FsPermission.getDefault() = 777.
         * 3. UMask is honored by underlying filesystem.
         */

        FsPermission.setUMask(conf, FsPermission.getDefault().applyUMask(
            tblPath.getFileSystem(conf).getFileStatus(tblPath).getPermission()));

      } catch(Exception e) {
        if( e instanceof HowlException ) {
          throw (HowlException) e;
        } else {
          throw new HowlException(ErrorType.ERROR_SET_OUTPUT, e);
        }
      } finally {
        if( client != null ) {
          client.close();
        }
      }
    }

    /**
     * Handles duplicate publish of partition. Fails if partition already exists.
     * For non partitioned tables, fails if files are present in table directory.
     * @param job the job
     * @param outputInfo the output info
     * @param client the metastore client
     * @param table the table being written to
     * @throws IOException
     * @throws MetaException
     * @throws TException
     */
    private static void handleDuplicatePublish(Job job, HowlTableInfo outputInfo,
        HiveMetaStoreClient client, Table table) throws IOException, MetaException, TException {
      List<String> partitionValues = HowlOutputCommitter.getPartitionValueList(
                  table, outputInfo.getPartitionValues());

      if( table.getPartitionKeys().size() > 0 ) {
        //For partitioned table, fail if partition is already present
        List<String> currentParts = client.listPartitionNames(outputInfo.getDatabaseName(),
            outputInfo.getTableName(), partitionValues, (short) 1);

        if( currentParts.size() > 0 ) {
          throw new HowlException(ErrorType.ERROR_DUPLICATE_PARTITION);
        }
      } else {
        Path tablePath = new Path(table.getSd().getLocation());
        FileSystem fs = tablePath.getFileSystem(job.getConfiguration());

        if ( fs.exists(tablePath) ) {
          FileStatus[] status = fs.globStatus(new Path(tablePath, "*"), hiddenFileFilter);

          if( status.length > 0 ) {
            throw new HowlException(ErrorType.ERROR_NON_EMPTY_TABLE,
                      table.getDbName() + "." + table.getTableName());
          }
        }
      }
    }

    /**
     * Set the schema for the data being written out to the partition. The
     * table schema is used by default for the partition if this is not called.
     * @param job the job object
     * @param schema the schema for the data
     */
    public static void setSchema(final Job job, final HowlSchema schema) throws IOException {

        OutputJobInfo jobInfo = getJobInfo(job);
        Map<String,String> partMap = jobInfo.getTableInfo().getPartitionValues();
        List<Integer> posOfPartCols = new ArrayList<Integer>();

        // If partition columns occur in data, we want to remove them.
        // So, find out positions of partition columns in schema provided by user.
        // We also need to update the output Schema with these deletions.

        // Note that, output storage drivers never sees partition columns in data
        // or schema.

        HowlSchema schemaWithoutParts = new HowlSchema(schema.getFields());
        for(String partKey : partMap.keySet()){
          Integer idx;
          if((idx = schema.getPosition(partKey)) != null){
            posOfPartCols.add(idx);
            schemaWithoutParts.remove(schema.get(partKey));
          }
        }
        HowlUtil.validatePartitionSchema(jobInfo.getTable(), schemaWithoutParts);
        jobInfo.setPosOfPartCols(posOfPartCols);
        jobInfo.setOutputSchema(schemaWithoutParts);
        job.getConfiguration().set(HOWL_KEY_OUTPUT_INFO, HowlUtil.serialize(jobInfo));
    }

    /**
     * Gets the table schema for the table specified in the HowlOutputFormat.setOutput call
     * on the specified job context.
     * @param context the context
     * @return the table schema
     * @throws IOException if HowlOutputFromat.setOutput has not been called for the passed context
     */
    public static HowlSchema getTableSchema(JobContext context) throws IOException {
        OutputJobInfo jobInfo = getJobInfo(context);
        return jobInfo.getTableSchema();
    }

    /**
     * Get the record writer for the job. Uses the Table's default OutputStorageDriver
     * to get the record writer.
     * @param context the information about the current task.
     * @return a RecordWriter to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<WritableComparable<?>, HowlRecord>
      getRecordWriter(TaskAttemptContext context
                      ) throws IOException, InterruptedException {

      // First create the RW.
      HowlRecordWriter rw = new HowlRecordWriter(context);

      // Now set permissions and group on freshly created files.
      OutputJobInfo info =  getJobInfo(context);
      Path workFile = rw.getStorageDriver().getWorkFilePath(context,info.getLocation());
      Path tblPath = new Path(info.getTable().getSd().getLocation());
      FileSystem fs = tblPath.getFileSystem(context.getConfiguration());
      FileStatus tblPathStat = fs.getFileStatus(tblPath);
      fs.setPermission(workFile, tblPathStat.getPermission());
      fs.setOwner(workFile, null, tblPathStat.getGroup());
      return rw;
    }

    /**
     * Check for validity of the output-specification for the job.
     * @param context information about the job
     * @throws IOException when output should not be attempted
     */
    @Override
    public void checkOutputSpecs(JobContext context
                                          ) throws IOException, InterruptedException {
        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getOutputFormat(context);
        outputFormat.checkOutputSpecs(context);
    }

    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context
                                       ) throws IOException, InterruptedException {
        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getOutputFormat(context);
        return new HowlOutputCommitter(outputFormat.getOutputCommitter(context));
    }


    /**
     * Gets the output format instance.
     * @param context the job context
     * @return the output format instance
     * @throws IOException
     */
    private OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat(JobContext context) throws IOException {
        OutputJobInfo jobInfo = getJobInfo(context);
        HowlOutputStorageDriver  driver = getOutputDriverInstance(context, jobInfo);

        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat =
              driver.getOutputFormat();
        return outputFormat;
    }

    /**
     * Gets the HowlOuputJobInfo object by reading the Configuration and deserializing
     * the string. If JobInfo is not present in the configuration, throws an
     * exception since that means HowlOutputFormat.setOutput has not been called.
     * @param jobContext the job context
     * @return the OutputJobInfo object
     * @throws IOException the IO exception
     */
    static OutputJobInfo getJobInfo(JobContext jobContext) throws IOException {
        String jobString = jobContext.getConfiguration().get(HOWL_KEY_OUTPUT_INFO);
        if( jobString == null ) {
            throw new HowlException(ErrorType.ERROR_NOT_INITIALIZED);
        }

        return (OutputJobInfo) HowlUtil.deserialize(jobString);
    }

    /**
     * Gets the output storage driver instance.
     * @param jobContext the job context
     * @param jobInfo the output job info
     * @return the output driver instance
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    static HowlOutputStorageDriver getOutputDriverInstance(
            JobContext jobContext, OutputJobInfo jobInfo) throws IOException {
        try {
            Class<? extends HowlOutputStorageDriver> driverClass =
                (Class<? extends HowlOutputStorageDriver>)
                Class.forName(jobInfo.getStorerInfo().getOutputSDClass());
            HowlOutputStorageDriver driver = driverClass.newInstance();

            //Initialize the storage driver
            driver.setSchema(jobContext, jobInfo.getOutputSchema());
            driver.setPartitionValues(jobContext, jobInfo.getTableInfo().getPartitionValues());
            driver.setOutputPath(jobContext, jobInfo.getLocation());

            driver.initialize(jobContext, jobInfo.getStorerInfo().getProperties());

            return driver;
        } catch(Exception e) {
            throw new HowlException(ErrorType.ERROR_INIT_STORAGE_DRIVER, e);
        }
    }

    static HiveMetaStoreClient createHiveClient(String url, Configuration conf) throws IOException, MetaException {
      HiveConf hiveConf = new HiveConf(HowlOutputFormat.class);

      if( url != null ) {
        //User specified a thrift url
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, url);
      } else {
        //Thrift url is null, copy the hive conf into the job conf and restore it
        //in the backend context

        if( conf.get(HOWL_KEY_HIVE_CONF) == null ) {
          conf.set(HOWL_KEY_HIVE_CONF, HowlUtil.serialize(hiveConf.getAllProperties()));
        } else {
          //Copy configuration properties into the hive conf
          Properties properties = (Properties) HowlUtil.deserialize(conf.get(HOWL_KEY_HIVE_CONF));

          for(Map.Entry<Object, Object> prop : properties.entrySet() ) {
            if( prop.getValue() instanceof String ) {
              hiveConf.set((String) prop.getKey(), (String) prop.getValue());
            } else if( prop.getValue() instanceof Integer ) {
              hiveConf.setInt((String) prop.getKey(), (Integer) prop.getValue());
            } else if( prop.getValue() instanceof Boolean ) {
              hiveConf.setBoolean((String) prop.getKey(), (Boolean) prop.getValue());
            } else if( prop.getValue() instanceof Long ) {
              hiveConf.setLong((String) prop.getKey(), (Long) prop.getValue());
            } else if( prop.getValue() instanceof Float ) {
              hiveConf.setFloat((String) prop.getKey(), (Float) prop.getValue());
            }
          }
        }

      }

      return new HiveMetaStoreClient(hiveConf);
    }


}
