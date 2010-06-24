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
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.impl.util.ObjectSerializer;

/** The InputFormat to use to read data from Owl */
public class HowlInputFormat extends InputFormat<WritableComparable, HowlRecord> {

  //The keys used to store info into the job Configuration
  static final String HOWL_KEY_BASE = "mapreduce.lib.howl";
  static final String HOWL_KEY_OUTPUT_SCHEMA = HOWL_KEY_BASE + ".output.schema";
  static final String HOWL_KEY_JOB_INFO =  HOWL_KEY_BASE + ".job.info";
  static final String HOWL_KEY_PREDICATE = HOWL_KEY_BASE + ".predicate.string";

  /**
   * Enumeration of possible Howl Operations
   */
  public enum HowlOperation {
    PREDICATE_PUSHDOWN,
    PROJECTION_PUSHDOWN
  }

  /**
   * Set the input to use for the Job. This queries the metadata server with
   * the specified partition predicates, gets the matching partitions, puts
   * the information in the conf object. The inputInfo object is updated with
   * information needed in the client context
   * @param job the job object
   * @param inputInfo the table input info
   * @throws IOException the exception in communicating with the metadata server
   */
  public static void setInput(Job job,
      HowlTableInfo inputInfo) throws IOException {
    try {
      InitializeInput.setInput(job, inputInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Gets the list of features supported for the given partitions. If any one
   * of the underlying InputFormat's does not support the feature and it cannot
   * be implemented by Howl, then the feature is not returned.
   * @param inputInfo  the owl table input info
   * @return the storage features supported for the partitions selected
   *         by the setInput call
   * @throws Exception
   */
  public static List<HowlOperation> getSupportedFeatures(
      HowlTableInfo inputInfo) throws Exception {
    return HowlFeatureSupport.getSupportedFeatures(inputInfo.getJobInfo());
  }

  /**
   * Checks if the specified operation is supported for the given partitions.
   * If any one of the underlying InputFormat's does not support the operation
   * and it cannot be implemented by Howl, then returns false. Else returns true.
   * @param inputInfo the owl table input info
   * @param operation the operation to check for
   * @return true, if the feature is supported for selected partitions
   * @throws IOException
   */
  public static boolean isFeatureSupported(HowlTableInfo inputInfo,
      HowlOperation operation) throws Exception {
    return HowlFeatureSupport.isFeatureSupported(inputInfo.getJobInfo(), operation);
  }

  /**
   * Set the predicate filter for pushdown to the storage driver.
   * @param job the job object
   * @param predicate the predicate filter, an arbitrary AND/OR filter
   * @return true, if the specified predicate can be filtered for the
   *         given partitions
   * @throws IOException the exception
   */
  public static boolean setPredicate(Job job,
      String predicate) throws Exception {
    return HowlFeatureSupport.setPredicate(job, predicate);
  }

  /**
   * Set the schema for the HowlRecord data returned by OwlInputFormat.
   * @param job the job object
   * @param howlSchema the schema to use as the consolidated schema
   */
  public static void setOutputSchema(Job job,
      Schema howlSchema) throws IOException {
    job.getConfiguration().set(
        HOWL_KEY_OUTPUT_SCHEMA, ObjectSerializer.serialize(howlSchema));
  }

  /**
   * Logically split the set of input files for the job. Returns the
   * underlying InputFormat's splits
   * @param jobContext the job context object
   * @return the splits, an OwlInputSplit wrapper over the storage
   *         driver InputSplits
   * @throws IOException or InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
  throws IOException, InterruptedException {

    //Get the job info from the configuration,
    //throws exception if not initialized
    JobInfo jobInfo;
    try {
      jobInfo = getJobInfo(jobContext);
    } catch (Exception e) {
      throw new IOException(e);
    }

    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<PartInfo> partitionInfoList = jobInfo.getPartitions();
    if(partitionInfoList == null ) {
      //No partitions match the specified partition filter
      return splits;
    }

    //For each matching partition, call getSplits on the underlying InputFormat
    for(PartInfo partitionInfo : partitionInfoList) {
      Job localJob = new Job(jobContext.getConfiguration());
      HowlInputStorageDriver storageDriver;
      try {
        storageDriver = getInputDriverInstance(partitionInfo.getLoaderInfo());
      } catch (Exception e) {
        throw new IOException(e);
      }

      //Pass all required information to the storage driver
      initStorageDriver(storageDriver, localJob, partitionInfo, jobInfo.getTableSchema());

      //Get the input format for the storage driver
      InputFormat inputFormat =
        storageDriver.getInputFormat(partitionInfo.getLoaderInfo());

      //Call getSplit on the storage drivers InputFormat, create an
      //OwlSplit for each underlying split
      List<InputSplit> baseSplits = inputFormat.getSplits(localJob);

      for(InputSplit split : baseSplits) {
        splits.add(new HowlSplit(
            partitionInfo,
            split,
            jobInfo.getTableSchema()));
      }
    }

    return splits;
  }

  /**
   * Create the RecordReader for the given InputSplit. Returns the underlying
   * RecordReader if the required operations are supported and schema matches
   * with OwlTable schema. Returns an OwlRecordReader if operations need to
   * be implemented in owl.
   * @param split the split
   * @param taskContext the task attempt context
   * @return the record reader instance, either an OwlRecordReader(later) or
   *         the underlying storage driver's RecordReader
   * @throws IOException or InterruptedException
   */
  @Override
  public RecordReader<WritableComparable, HowlRecord> createRecordReader(InputSplit split,
      TaskAttemptContext taskContext) throws IOException, InterruptedException {

    HowlSplit howlSplit = (HowlSplit) split;
    PartInfo partitionInfo = howlSplit.getPartitionInfo();

    //If running through a Pig job, the JobInfo will not be available in the
    //backend process context (since OwlLoader works on a copy of the JobContext and does
    //not call OwlInputFormat.setInput in the backend process).
    //So this function should NOT attempt to read the JobInfo.

    HowlInputStorageDriver storageDriver;
    try {
      storageDriver = getInputDriverInstance(partitionInfo.getLoaderInfo());
    } catch (Exception e) {
      throw new IOException(e);
    }

    //Pass all required information to the storage driver
    initStorageDriver(storageDriver, taskContext, partitionInfo, howlSplit.getTableSchema());

    //Get the input format for the storage driver
    InputFormat inputFormat =
      storageDriver.getInputFormat(partitionInfo.getLoaderInfo());

    //Create the underlying input formats record record and an Owl wrapper
    RecordReader recordReader =
      inputFormat.createRecordReader(howlSplit.getBaseSplit(), taskContext);

    return new HowlRecordReader(storageDriver,recordReader);
  }

  /**
   * Gets the OwlTable schema for the table specified in the OwlInputFormat.setInput call
   * on the specified job context. This information is available only after OwlInputFormat.setInput
   * has been called for a JobContext.
   * @param context the context
   * @return the table schema
   * @throws Exception if OwlInputFromat.setInput has not been called for the current context
   */
  public static Schema getTableSchema(JobContext context) throws Exception {
    JobInfo jobInfo = getJobInfo(context);
    return jobInfo.getTableSchema();
  }

  /**
   * Gets the JobInfo object by reading the Configuration and deserializing
   * the string. If JobInfo is not present in the configuration, throws an
   * exception since that means OwlInputFormat.setInput has not been called.
   * @param jobContext the job context
   * @return the JobInfo object
   * @throws Exception the owl exception
   */
  private static JobInfo getJobInfo(JobContext jobContext) throws Exception {
    String jobString = jobContext.getConfiguration().get(HOWL_KEY_JOB_INFO);
    if( jobString == null ) {
      throw new Exception("Input not initialized from jobContext");
    }

    return (JobInfo) ObjectSerializer.deserialize(jobString);
  }


  /**
   * Initializes the storage driver instance. Passes on the required
   * schema information, path info and arguments for the supported
   * features to the storage driver.
   * @param storageDriver the storage driver
   * @param context the job context
   * @param partitionInfo the partition info
   * @param tableSchema the owl table level schema
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void initStorageDriver(HowlInputStorageDriver storageDriver,
      JobContext context, PartInfo partitionInfo,
      Schema tableSchema) throws IOException {

    storageDriver.setInputPath(context, partitionInfo.getLocation());

    if( partitionInfo.getPartitionSchema() != null ) {
      storageDriver.setOriginalSchema(context, partitionInfo.getPartitionSchema());
    }

    storageDriver.setPartitionValues(context, partitionInfo.getPartitionValues());

    //Set the output schema. Use the schema given by user if set, otherwise use the
    //table level schema
    Schema outputSchema = null;
    String outputSchemaString = context.getConfiguration().get(HOWL_KEY_OUTPUT_SCHEMA);
    if( outputSchemaString != null ) {
      outputSchema = (Schema) ObjectSerializer.deserialize(outputSchemaString);
    } else {
      outputSchema = tableSchema;
    }

    storageDriver.setOutputSchema(context, outputSchema);

    //If predicate is set in jobConf, pass it to storage driver
    String predicate = context.getConfiguration().get(HOWL_KEY_PREDICATE);
    if( predicate != null ) {
      storageDriver.setPredicate(context, predicate);
    }

    storageDriver.initialize(context, partitionInfo.getLoaderInfo());
  }

  /**
   * Gets the input driver instance.
   * @param loaderInfo the loader info
   * @return the input driver instance
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  private HowlInputStorageDriver getInputDriverInstance(
      LoaderInfo loaderInfo) throws Exception {
    try {
      Class<? extends HowlInputStorageDriver> driverClass =
        (Class<? extends HowlInputStorageDriver>)
        Class.forName(loaderInfo.getInputDriverClass());
      return driverClass.newInstance();
    } catch(Exception e) {
      throw new Exception("error creating storage driver " +
          loaderInfo.getInputDriverClass(), e);
    }
  }
}
