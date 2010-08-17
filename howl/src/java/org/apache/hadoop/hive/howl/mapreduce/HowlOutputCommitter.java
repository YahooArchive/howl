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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.howl.common.ErrorType;
import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;

public class HowlOutputCommitter extends OutputCommitter {

    /** The underlying output committer */
    private final OutputCommitter baseCommitter;

    public HowlOutputCommitter(OutputCommitter baseCommitter) {
        this.baseCommitter = baseCommitter;
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        baseCommitter.abortTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        baseCommitter.commitTask(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return baseCommitter.needsTaskCommit(context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
      if( baseCommitter != null ) {
        baseCommitter.setupJob(context);
      }
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        baseCommitter.setupTask(context);
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {
      OutputJobInfo jobInfo = HowlOutputFormat.getJobInfo(context);

      if( jobInfo.getTable().getPartitionKeys().size() == 0 ) {
        //non partitioned table

        if( baseCommitter != null ) {
          baseCommitter.cleanupJob(context);
        }

        //Move data from temp directory the actual table directory
        //No metastore operation required.
        Path src = new Path(jobInfo.getLocation());
        Path dest = new Path(jobInfo.getTable().getSd().getLocation());
        FileSystem fs = src.getFileSystem(context.getConfiguration());

        moveTaskOutputs(fs, src, src, dest);
        fs.delete(src, true);
        return;
      }

      HiveMetaStoreClient client = null;
      List<String> values = null;
      boolean partitionAdded = false;
      HowlTableInfo tableInfo = jobInfo.getTableInfo();

      try {
        client = HowlOutputFormat.createHiveClient(
            jobInfo.getTableInfo().getServerUri(), context.getConfiguration());

        Table table = client.getTable(tableInfo.getDatabaseName(), tableInfo.getTableName());
        StorerInfo storer = InitializeInput.extractStorerInfo(table.getParameters());

        Partition partition = new Partition();
        partition.setDbName(tableInfo.getDatabaseName());
        partition.setTableName(tableInfo.getTableName());
        partition.setSd(new StorageDescriptor(table.getSd()));
        partition.getSd().setLocation(jobInfo.getLocation());

        updateTableSchema(client, table, jobInfo.getOutputSchema());

        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for(HowlFieldSchema fieldSchema : jobInfo.getOutputSchema().getHowlFieldSchemas()) {
          fields.add(fieldSchema);
        }

        partition.getSd().setCols(fields);

        //Get partition value list
        values = getPartitionValueList(table,
            jobInfo.getTableInfo().getPartitionValues());
        partition.setValues(values);

        Map<String, String> params = new HashMap<String, String>();
        params.put(InitializeInput.HOWL_ISD_CLASS, storer.getInputSDClass());
        params.put(InitializeInput.HOWL_OSD_CLASS, storer.getOutputSDClass());

        //Copy table level howl.* keys to the partition
        for(Map.Entry<Object, Object> entry : storer.getProperties().entrySet()) {
          params.put(entry.getKey().toString(), entry.getValue().toString());
        }

        partition.setParameters(params);
        //Publish the new partition
        client.add_partition(partition);
        partitionAdded = true; //publish to metastore done

        if( baseCommitter != null ) {
          baseCommitter.cleanupJob(context);
        }
      } catch (Exception e) {

        if( partitionAdded ) {
          try {
            //baseCommitter.cleanupJob failed, try to clean up the metastore
            client.dropPartition(tableInfo.getDatabaseName(),
                    tableInfo.getTableName(), values);
          } catch(Exception te) {
            //Keep cause as the original exception
            throw new HowlException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
          }
        }

        if( e instanceof HowlException ) {
          throw (HowlException) e;
        } else {
          throw new HowlException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      } finally {
        if( client != null ) {
          client.close();
        }
      }
    }


    /**
     * Validate partition schema, checks if the column types match between the partition
     * and the existing table schema. Returns the list of columns present in the partition
     * but not in the table.
     * @param table the table
     * @param partitionSchema the partition schema
     * @return the list of newly added fields
     * @throws IOException Signals that an I/O exception has occurred.
     */
    static List<FieldSchema> validatePartitionSchema(Table table, HowlSchema partitionSchema) throws IOException {
      Map<String, FieldSchema> partitionKeyMap = new HashMap<String, FieldSchema>();

      for(FieldSchema field : table.getPartitionKeys()) {
        partitionKeyMap.put(field.getName().toLowerCase(), field);
      }

      List<FieldSchema> tableCols = table.getSd().getCols();
      List<FieldSchema> newFields = new ArrayList<FieldSchema>();

      for(int i = 0;i <  partitionSchema.getHowlFieldSchemas().size();i++) {

        HowlFieldSchema field = partitionSchema.getHowlFieldSchemas().get(i);

        FieldSchema tableField;
        if( i < tableCols.size() ) {
          tableField = tableCols.get(i);

          if( ! tableField.getName().equalsIgnoreCase(field.getName())) {
            throw new HowlException(ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH, "Expected column <" + tableField.getName() +
                "> at position " + (i + 1) + ", found column <" + field.getName() + ">");
          }
        } else {
          tableField = partitionKeyMap.get(field.getName().toLowerCase());

          if( tableField != null ) {
            throw new HowlException(ErrorType.ERROR_SCHEMA_PARTITION_KEY, "Key <" +  field.getName() + ">");
          }
        }

        if( tableField == null ) {
          //field present in partition but not in table
          newFields.add(field);
        } else {
          //field present in both. validate type has not changed
          TypeInfo partitionType = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
          TypeInfo tableType = TypeInfoUtils.getTypeInfoFromTypeString(tableField.getType());

          if( ! partitionType.equals(tableType) ) {
            throw new HowlException(ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, "Column <" + field.getName() + ">, expected <" +
                tableType.getTypeName() + ">, got <" + partitionType.getTypeName() + ">");
          }
        }
      }

      return newFields;
    }


    /**
     * Update table schema, adding new columns as added for the partition.
     * @param client the client
     * @param table the table
     * @param partitionSchema the schema of the partition
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InvalidOperationException the invalid operation exception
     * @throws MetaException the meta exception
     * @throws TException the t exception
     */
    private void updateTableSchema(HiveMetaStoreClient client, Table table,
        HowlSchema partitionSchema) throws IOException, InvalidOperationException, MetaException, TException {

      List<FieldSchema> newColumns = validatePartitionSchema(table, partitionSchema);

      if( newColumns.size() != 0 ) {
        List<FieldSchema> tableColumns = new ArrayList<FieldSchema>(table.getSd().getCols());
        tableColumns.addAll(newColumns);

        //Update table schema to add the newly added columns
        table.getSd().setCols(tableColumns);
        client.alter_table(table.getDbName(), table.getTableName(), table);
      }
    }

    /**
     * Convert the partition value map to a value list in the partition key order.
     * @param table the table being written to
     * @param valueMap the partition value map
     * @return the partition value list
     * @throws IOException
     */
    static List<String> getPartitionValueList(Table table, Map<String, String> valueMap) throws IOException {

      if( valueMap.size() != table.getPartitionKeys().size() ) {
          throw new HowlException(ErrorType.ERROR_INVALID_PARTITION_VALUES,
              "Table "
              + table.getTableName() + " has " +
              table.getPartitionKeys().size() + " partition keys, got "+
              valueMap.size());
      }

      List<String> values = new ArrayList<String>();

      for(FieldSchema schema : table.getPartitionKeys()) {
        String value = valueMap.get(schema.getName().toLowerCase());

        if( value == null ) {
          throw new HowlException(ErrorType.ERROR_MISSING_PARTITION_KEY,
              "Key " + schema.getName() + " of table " + table.getTableName());
        }

        values.add(value);
      }

      return values;
    }

    /**
     * Move all of the files from the temp directory to the final location
     * @param fs the output file system
     * @param file the file to move
     * @param src the source directory
     * @param dest the target directory
     * @throws IOException
     */
    private void moveTaskOutputs(FileSystem fs,
                                 Path file,
                                 Path src,
                                 Path dest) throws IOException {
      if (fs.isFile(file)) {
        Path finalOutputPath = getFinalPath(file, src, dest);

        if (!fs.rename(file, finalOutputPath)) {
          if (!fs.delete(finalOutputPath, true)) {
            throw new HowlException(ErrorType.ERROR_MOVE_FAILED, "Failed to delete existing path " + finalOutputPath);
          }
          if (!fs.rename(file, finalOutputPath)) {
            throw new HowlException(ErrorType.ERROR_MOVE_FAILED, "Failed to move output to " + dest);
          }
        }
      } else if(fs.getFileStatus(file).isDir()) {
        FileStatus[] paths = fs.listStatus(file);
        Path finalOutputPath = getFinalPath(file, src, dest);
        fs.mkdirs(finalOutputPath);

        if (paths != null) {
          for (FileStatus path : paths) {
            moveTaskOutputs(fs, path.getPath(), src, dest);
          }
        }
      }
    }

    /**
     * Find the final name of a given output file, given the output directory
     * and the work directory.
     * @param file the file to move
     * @param src the source directory
     * @param dest the target directory
     * @return the final path for the specific output file
     * @throws IOException
     */
    private Path getFinalPath(Path file, Path src,
                              Path dest) throws IOException {
      URI taskOutputUri = file.toUri();
      URI relativePath = src.toUri().relativize(taskOutputUri);
      if (taskOutputUri == relativePath) {
        throw new HowlException(ErrorType.ERROR_MOVE_FAILED, "Can not get the relative path: base = " +
            src + " child = " + file);
      }
      if (relativePath.getPath().length() > 0) {
        return new Path(dest, relativePath.getPath());
      } else {
        return dest;
      }
    }

}
