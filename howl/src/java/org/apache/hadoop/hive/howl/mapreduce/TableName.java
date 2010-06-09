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


public class TableName {

  private String dbName = null;
  private String tableName = null;

  public TableName(String dbName, String tableName) {
    this.setDatabaseName(dbName);
    this.setTableName(tableName);
  }

  /**
   * @param dbName the dbName to set
   */
  public void setDatabaseName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @return the dbName
   */
  public String getDatabaseName() {
    return dbName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
      + ((dbName == null) ? 0 : dbName.hashCode());
      result = prime * result
      + ((tableName == null) ? 0 : tableName.hashCode());
      return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
      if (this == obj) {
          return true;
      }
      if (obj == null) {
          return false;
      }
      if (getClass() != obj.getClass()) {
          return false;
      }
      TableName other = (TableName) obj;
      if (dbName == null) {
          if (other.dbName != null) {
              return false;
          }
      } else if (!dbName.equals(other.dbName)) {
          return false;
      }
      if (tableName == null) {
          if (other.tableName != null) {
              return false;
          }
      } else if (!tableName.equals(other.tableName)) {
          return false;
      }
      return true;
  }

};
