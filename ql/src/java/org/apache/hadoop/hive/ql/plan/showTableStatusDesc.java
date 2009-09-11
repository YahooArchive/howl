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

@explain(displayName = "Show Table Status")
public class showTableStatusDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String pattern;
  Path resFile;
  String dbName;
  HashMap<String, String> partSpec;
  
  /**
   * table name for the result of show tables
   */
  private final String table = "show_tablestatus";
  /**
   * thrift ddl for the result of show tables
   */
  private final String schema = "tab_name#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * @param pattern
   *          names of tables to show
   */
  public showTableStatusDesc(Path resFile, String dbName, String pattern) {
    this.dbName = dbName;
    this.resFile = resFile;
    this.pattern = pattern;
  }

  /**
   * @param resFile
   * @param dbName data base name
   * @param pattern names of tables to show
   * @param part partition specification
   */
  public showTableStatusDesc(Path resFile, String dbName, String pattern,
      HashMap<String, String> partSpec) {
    this.dbName = dbName;
    this.resFile = resFile;
    this.pattern = pattern;
    this.partSpec = partSpec;
  }

  /**
   * @return the pattern
   */
  @explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  /**
   * @param pattern
   *          the pattern to set
   */
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * @return the resFile
   */
  public Path getResFile() {
    return resFile;
  }

  @explain(displayName = "result file", normalExplain = false)
  public String getResFileString() {
    return getResFile().getName();
  }

  /**
   * @param resFile
   *          the resFile to set
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
  
  /**
   * @return the database name
   */
  @explain(displayName = "database")
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName
   *          the database name
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }
  
  /**
   * @return the partSpec
   */
  @explain(displayName="partition")
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec the partSpec to set
   */
  public void setPartSpecs(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }
}
