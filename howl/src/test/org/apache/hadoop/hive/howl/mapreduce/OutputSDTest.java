/**
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
import java.util.Properties;

import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;

public class OutputSDTest extends HowlOutputStorageDriver {

  @Override
  public Writable convertValue(HowlRecord value) {
    return value;
  }

  @Override
  public void setOutputPath(JobContext jobContext, String location) throws IOException {
  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {
  }

  @Override
  public void setSchema(JobContext jobContext, Schema schema) throws IOException {
  }

  @Override
  public WritableComparable<?> generateKey(HowlRecord value) {
    return null;
  }

  @Override
  public OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat(
      Properties properties) {
    return null;
  }

}
