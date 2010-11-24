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

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.howl.data.schema.HowlSchema;

/** The Class used to serialize the partition information read from the metadata server that maps to a partition */
public class PartInfo implements Serializable {

  /** The serialization version */
  private static final long serialVersionUID = 1L;

  /** The partition schema. */
  private final HowlSchema partitionSchema;

  /** The information about which input storage driver to use */
  private final String inputStorageDriverClass;

  /** Howl-specific properties set at the partition */
  private final Properties howlProperties;

  /** The data location. */
  private final String location;

  /** The map of partition key names and their values. */
  private Map<String,String> partitionValues;

  /**
   * Instantiates a new howl partition info.
   * @param partitionSchema the partition schema
   * @param inputStorageDriverClass the input storage driver class name
   * @param location the location
   * @param howlProperties howl-specific properties at the partition
   */
  public PartInfo(HowlSchema partitionSchema, String inputStorageDriverClass, String location, Properties howlProperties){
    this.partitionSchema = partitionSchema;
    this.inputStorageDriverClass = inputStorageDriverClass;
    this.location = location;
    this.howlProperties = howlProperties;
  }

  /**
   * Gets the value of partitionSchema.
   * @return the partitionSchema
   */
  public HowlSchema getPartitionSchema() {
    return partitionSchema;
  }


  /**
   * Gets the value of input storage driver class name.
   * @return the input storage driver class name
   */
  public String getInputStorageDriverClass() {
    return inputStorageDriverClass;
  }


  /**
   * Gets the value of howlProperties.
   * @return the howlProperties
   */
  public Properties getInputStorageDriverProperties() {
    return howlProperties;
  }

  /**
   * Gets the value of location.
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * Sets the partition values.
   * @param partitionValues the new partition values
   */
  public void setPartitionValues(Map<String,String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  /**
   * Gets the partition values.
   * @return the partition values
   */
  public Map<String,String> getPartitionValues() {
    return partitionValues;
  }
}
