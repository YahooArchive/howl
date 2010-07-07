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

import org.apache.hadoop.hive.metastore.api.Schema;

/** The class used to serialize and store the output related information  */
class OutputJobInfo implements Serializable {

    /** The serialization version. */
    private static final long serialVersionUID = 1L;

    /** The table info provided by user. */
    private HowlTableInfo tableInfo;

    /** The output schema. */
    private Schema outputSchema;

    /** The table level schema. */
    private Schema tableSchema;

    /** The storer info */
    private StorerInfo storerInfo;

    /** The location of the partition being written */
    private String location;

    public OutputJobInfo(HowlTableInfo tableInfo, Schema outputSchema, Schema tableSchema,
        StorerInfo storerInfo, String location) {
      super();
      this.tableInfo = tableInfo;
      this.outputSchema = outputSchema;
      this.tableSchema = tableSchema;
      this.storerInfo = storerInfo;
      this.location = location;
    }

    /**
     * @return the tableInfo
     */
    public HowlTableInfo getTableInfo() {
      return tableInfo;
    }

    /**
     * @param tableInfo the tableInfo to set
     */
    public void setTableInfo(HowlTableInfo tableInfo) {
      this.tableInfo = tableInfo;
    }

    /**
     * @return the outputSchema
     */
    public Schema getOutputSchema() {
      return outputSchema;
    }

    /**
     * @param outputSchema the outputSchema to set
     */
    public void setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
    }

    /**
     * @return the tableSchema
     */
    public Schema getTableSchema() {
      return tableSchema;
    }

    /**
     * @param tableSchema the tableSchema to set
     */
    public void setTableSchema(Schema tableSchema) {
      this.tableSchema = tableSchema;
    }

    /**
     * @return the storerInfo
     */
    public StorerInfo getStorerInfo() {
      return storerInfo;
    }

    /**
     * @param storerInfo the storerInfo to set
     */
    public void setStorerInfo(StorerInfo storerInfo) {
      this.storerInfo = storerInfo;
    }

    /**
     * @param location the location to set
     */
    public void setLocation(String location) {
      this.location = location;
    }

    /**
     * @return the location
     */
    public String getLocation() {
      return location;
    }

}
