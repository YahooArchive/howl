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
package org.apache.hadoop.hive.howl.drivers;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver;
import org.apache.hadoop.hive.io.RCFileMapReduceOutputFormat;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * The storage driver for writing RCFile data through HowlOutputFromat.
 */
 public class RCFileOutputStorageDriver extends HowlOutputStorageDriver {

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#convertValue(org.apache.hadoop.hive.howl.data.HowlRecord)
   */
  @Override
  public Writable convertValue(HowlRecord value) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#generateKey(org.apache.hadoop.hive.howl.data.HowlRecord)
   */
  @Override
  public WritableComparable<?> generateKey(HowlRecord value) {
    //key is not used for RCFile output
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#getOutputFormat(java.util.Properties)
   */
  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat(
      Properties properties) {
    return (OutputFormat) new RCFileMapReduceOutputFormat();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setOutputPath(org.apache.hadoop.mapreduce.JobContext, java.lang.String)
   */
  @Override
  public void setOutputPath(JobContext jobContext, String location) throws IOException {
    //Not calling FileOutputFormat.setOutputPath since that requires a Job instead of JobContext
    jobContext.getConfiguration().set("mapred.output.dir", location);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setPartitionValues(org.apache.hadoop.mapreduce.JobContext, java.util.Map)
   */
  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {
    //default implementation of HowlOutputStorageDriver.getPartitionLocation will use the partition
    //values to generate the data location, so partition values not used here
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setSchema(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.hive.metastore.api.Schema)
   */
  @Override
  public void setSchema(JobContext jobContext, Schema schema) throws IOException {
    //schema is not passed to RCFileOutputFormat, so ignored
  }

}
