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
import java.util.Map;

import org.apache.hadoop.hive.howl.mapreduce.HowlInputFormat.HowlOperation;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.data.Tuple;



/** The abstract class to be implemented by underlying storage drivers to enable data access from Owl through
 *  OwlInputFormat.
 */
public abstract class HowlInputStorageDriver {

  public enum State {
    INSTANTIATED_FROM_CREATE_RECORD_READER,
    INSTANTIATED_FROM_GET_INPUT_SPLITS
  };

  public void initialize(LoaderInfo loaderInfo, State instantiationState){
    // trivial do nothing
  }

  /**
   * Returns the InputFormat to use with this Storage Driver.
   * @param loaderInfo the loader info object containing parameters required for initialization of InputFormat
   * @return the InputFormat instance
   */
  public abstract InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(LoaderInfo loaderInfo, State instantiationState);

  /**
   * Converts value to Tuple format usable by HowlInputFormat to convert to required valuetype.
   * Implementers of StorageDriver should look to overwriting this function so as to convert their
   * value type to Tuple. Default implementation is provided for StorageDriver implementations
   * on top of an underlying InputFormat that already uses Tuple as a tuple
   * @param value the underlying value to convert to Tuple
   */
  public Tuple convertValueToTuple(Writable value) throws IOException {
    return (Tuple) value;
  }

  /**
   * Returns true if this InputFormat supports specified operation.
   * By default, returns false - the StorageDriver implementor is
   * expected to override to check for features they implement.
   * @param operation the operation to check for
   * @return true, if specified operation is supported
   */
  public boolean isFeatureSupported(HowlOperation operation) throws IOException{
    return false;
  }

  /**
   * Set the data location for the input.
   * @param jobContext the job context object
   * @param location the data location
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setInputPath(JobContext jobContext, String location, State instantiationState) throws IOException;

  /**
   * Set the predicate filter to be pushed down to the storage driver.
   * @param jobContext the job context object
   * @param predicate the predicate filter, an arbitrary AND/OR filter
   * @return true, if filtering for the specified predicate is supported. Default implementation in
   *               HowlInputStorageDriver  always returns false.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public boolean setPredicate(JobContext jobContext, String predicate) throws IOException {
    return false;
  }

  /**
   * Set the schema of the data as originally published in Owl. The storage driver might validated that this matches with
   * the schema it has (like Zebra) or it will use this to create a Tuple matching the output schema.
   * @param jobContext the job context object
   * @param howlSchema the schema published in Owl for this data
   * @param instantiationState
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setOriginalSchema(JobContext jobContext, HowlSchema howlSchema, State instantiationState) throws IOException;

  /**
   * Set the consolidated schema for the Tuple data returned by the storage driver. All tuples returned by the RecordReader should
   * have this schema. Nulls should be inserted for columns not present in the data.
   * @param jobContext the job context object
   * @param howlSchema the schema to use as the consolidated schema
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setOutputSchema(JobContext jobContext, HowlSchema howlSchema, State instantiationState) throws IOException;

  /**
   * Sets the partition key values for the current partition. The storage driver is passed this so that the storage
   * driver can add the partition key values to the output Tuple if the partition key values are not present on disk.
   * @param jobContext the job context object
   * @param partitionValues the partition values having a map with partition key name as key and the OwlKeyValue as value
   * @param instantiationState
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public abstract void setPartitionValues(JobContext jobContext, Map<String,String> partitionValues, State instantiationState) throws IOException;

}
