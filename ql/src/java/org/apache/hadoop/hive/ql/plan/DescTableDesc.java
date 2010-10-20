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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

/**
 * DescTableDesc.
 *
 */
@Explain(displayName = "Describe Table")
public class DescTableDesc extends DDLDesc implements Serializable {
  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  private static final long serialVersionUID = 1L;

  String tableName;
  HashMap<String, String> partSpec;
  String resFile;
  boolean isExt;
  boolean isFormatted;
  /**
   * table name for the result of describe table.
   */
  private static final String table = "describe";
  /**
   * thrift ddl for the result of describe table.
   */
  private static final String schema = "col_name,data_type,comment#string:string:string";

  public DescTableDesc() {
  }

  /**
   * @param partSpec
   * @param resFile
   * @param tableName
   */
  public DescTableDesc(Path resFile, String tableName,
      HashMap<String, String> partSpec) {
    this.isExt = false;
    this.isFormatted = false;
    this.partSpec = partSpec;
    this.resFile = resFile.toString();
    this.tableName = tableName;
  }

  public String getTable() {
    return table;
  }

  public static String getSchema() {
    return schema;
  }

  /**
   * @return the isExt
   */
  public boolean isExt() {
    return isExt;
  }

  /**
   * @param isExt
   *          the isExt to set
   */
  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

    /**
   * @return the isFormatted
   */
  public boolean isFormatted() {
    return isFormatted;
  }

  /**
   * @param isFormatted
   *          the isFormat to set
   */
  public void setFormatted(boolean isFormat) {
    this.isFormatted = isFormat;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "table")
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the partSpec
   */
  @Explain(displayName = "partition")
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   *          the partSpec to set
   */
  public void setPartSpecs(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return the resFile
   */
  @Explain(displayName = "result file", normalExplain = false)
  public String getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          the resFile to set
   */
  public void setResFile(String resFile) {
    this.resFile = resFile;
  }
}
