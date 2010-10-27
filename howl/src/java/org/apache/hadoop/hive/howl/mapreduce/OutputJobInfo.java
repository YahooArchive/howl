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
import java.util.List;

import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.metastore.api.Table;

/** The class used to serialize and store the output related information  */
class OutputJobInfo implements Serializable {

    /** The serialization version. */
    private static final long serialVersionUID = 1L;

    /** The table info provided by user. */
    private final HowlTableInfo tableInfo;

    /** The output schema. This is given to us by user.  This wont contain any
     * partition columns ,even if user has specified them.
     * */
    private HowlSchema outputSchema;

    /** This is table schema, retrieved from metastore. */
    private final HowlSchema tableSchema;

    /** The storer info */
    private final StorerInfo storerInfo;

    /** The location of the partition being written */
    private final String location;

    /** The table being written to */
    private final Table table;

    /** This is a list of partition columns which will be deleted from data, if
     * data contains partition columns.*/

    private List<Integer> posOfPartCols;

    /**
     * @return the posOfPartCols
     */
    protected List<Integer> getPosOfPartCols() {
      return posOfPartCols;
    }

    /**
     * @param posOfPartCols the posOfPartCols to set
     */
    protected void setPosOfPartCols(List<Integer> posOfPartCols) {
      this.posOfPartCols = posOfPartCols;
    }

    public OutputJobInfo(HowlTableInfo tableInfo, HowlSchema outputSchema, HowlSchema tableSchema,
        StorerInfo storerInfo, String location, Table table) {
      super();
      this.tableInfo = tableInfo;
      this.outputSchema = outputSchema;
      this.tableSchema = tableSchema;
      this.storerInfo = storerInfo;
      this.location = location;
      this.table = table;
    }

    /**
     * @return the tableInfo
     */
    public HowlTableInfo getTableInfo() {
      return tableInfo;
    }

    /**
     * @return the outputSchema
     */
    public HowlSchema getOutputSchema() {
      return outputSchema;
    }

    /**
     * @param schema the outputSchema to set
     */
    public void setOutputSchema(HowlSchema schema) {
      this.outputSchema = schema;
    }

    /**
     * @return the tableSchema
     */
    public HowlSchema getTableSchema() {
      return tableSchema;
    }

    /**
     * @return the storerInfo
     */
    public StorerInfo getStorerInfo() {
      return storerInfo;
    }

    /**
     * @return the location
     */
    public String getLocation() {
      return location;
    }

    /**
     * Gets the value of table
     * @return the table
     */
    public Table getTable() {
      return table;
    }

}
