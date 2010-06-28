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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.thrift.TException;

/** The OutputFormat to use to write data to Howl */
public class HowlOutputFormat extends OutputFormat<WritableComparable<?>, HowlRecord> {

    //The keys used to store info into the job Configuration
    static final String HOWL_KEY_OUTPUT_BASE = "mapreduce.lib.howloutput";
    static final String HOWL_KEY_OUTPUT_INFO = HOWL_KEY_OUTPUT_BASE + ".info";
    static final String HOWL_KEY_OUTPUT_TABLE_SCHEMA = HOWL_KEY_OUTPUT_BASE + ".table.schema";

    /**
     * Set the info about the output to write for the Job. This queries the metadata server
     * to find the StorageDriver to use for the table.  Throws error if partition is already published.
     * @param job the job object
     * @param outputInfo the table output info
     * @throws IOException the exception in communicating with the metadata server
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws TException
     */
    public static void setOutput(Job job, HowlTableInfo outputInfo) throws IOException, MetaException, TException, NoSuchObjectException {
      HiveMetaStoreClient client = createHiveClient(outputInfo.getServerUri(), job.getConfiguration());
      Table table = client.getTable(outputInfo.getDatabaseName(), outputInfo.getTableName());

      Schema tableSchema = InitializeInput.extractSchemaFromStorageDescriptor(table.getSd());
      StorerInfo storerInfo = InitializeInput.extractStorerInfo(table.getSd());

      OutputJobInfo jobInfo = new OutputJobInfo(outputInfo, tableSchema, tableSchema, storerInfo);
      job.getConfiguration().set(HOWL_KEY_OUTPUT_INFO, ObjectSerializer.serialize(jobInfo));
    }

    /**
     * Set the schema for the data being written out to the partition.
     * @param job the job object
     * @param schema the schema for the data
     */
    public static void setSchema(Job job, Schema schema) throws IOException {
        OutputJobInfo jobInfo = getJobInfo(job);
        jobInfo.setOutputSchema(schema);
        job.getConfiguration().set(HOWL_KEY_OUTPUT_INFO, ObjectSerializer.serialize(jobInfo));
    }

    /**
     * Gets the table schema for the table specified in the HowlOutputFormat.setOutput call
     * on the specified job context.
     * @param context the context
     * @return the table schema
     * @throws IOlException if HowlOutputFromat.setOutput has not been called for the passed context
     */
    public static Schema getTableSchema(JobContext context) throws IOException {
        OutputJobInfo jobInfo = getJobInfo(context);
        return jobInfo.getTableSchema();
    }

    /**
     * Get the record writer for the job. Uses the OwlTable's default OutputStorageDriver
     * to get the record writer.
     * @param context the information about the current task.
     * @return a RecordWriter to write the output for the job.
     * @throws IOException
     */
    @Override
    public RecordWriter<WritableComparable<?>, HowlRecord>
      getRecordWriter(TaskAttemptContext context
                      ) throws IOException, InterruptedException {

        OutputJobInfo jobInfo = getJobInfo(context);
        HowlOutputStorageDriver driver = getOutputDriverInstance(jobInfo.getStorerInfo());

        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = driver.getOutputFormat(jobInfo.getStorerInfo());
        return new HowlRecordWriter(driver, outputFormat.getRecordWriter(context));
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
     * @param storerInfo the storer info
     * @return the output driver instance
     * @throws OwlException
     */
    private OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat(JobContext context) throws IOException {
        OutputJobInfo jobInfo = getJobInfo(context);
        HowlOutputStorageDriver driver = getOutputDriverInstance(jobInfo.getStorerInfo());

        OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = driver.getOutputFormat(jobInfo.getStorerInfo());
        return outputFormat;
    }

    /**
     * Gets the HowlOuputJobInfo object by reading the Configuration and deserializing
     * the string. If JobInfo is not present in the configuration, throws an
     * exception since that means HowlOutputFormat.setOutput has not been called.
     * @param jobContext the job context
     * @return the HowlOutputJobInfo object
     * @throws OwlException the owl exception
     */
    static OutputJobInfo getJobInfo(JobContext jobContext) throws IOException {
        String jobString = jobContext.getConfiguration().get(HOWL_KEY_OUTPUT_INFO);
        if( jobString == null ) {
            throw new IOException("HowlOutputFormat not initialized, setOutput has to be called");
        }

        return (OutputJobInfo) ObjectSerializer.deserialize(jobString);
    }

    /**
     * Gets the output storage driver instance.
     * @param storerInfo the storer info
     * @return the output driver instance
     * @throws OwlException
     */
    @SuppressWarnings("unchecked")
    static HowlOutputStorageDriver getOutputDriverInstance(
            StorerInfo storerInfo) throws IOException {
        try {
            Class<? extends HowlOutputStorageDriver> driverClass =
                (Class<? extends HowlOutputStorageDriver>)
                Class.forName(storerInfo.getOutputDriverClass());
            return driverClass.newInstance();
        } catch(Exception e) {
            throw new IOException("Error initializing output storage driver instance", e);
        }
    }

    static HiveMetaStoreClient createHiveClient(String url, Configuration conf) throws MetaException {
      HiveConf hiveConf = new HiveConf(HowlOutputFormat.class);

      if( url != null ) {
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.set(HiveConf.ConfVars.METATORETHRIFTRETRIES.varname, "2");
        hiveConf.set("hive.metastore.uris", url);
      }

      return new HiveMetaStoreClient(hiveConf);
    }

}
