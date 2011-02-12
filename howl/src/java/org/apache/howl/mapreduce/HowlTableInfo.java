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

package org.apache.howl.mapreduce;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;

/**
 *
 * HowlTableInfo - class to communicate table information to {@link HowlInputFormat}
 * and {@link HowlOutputFormat}
 *
 */
public class HowlTableInfo implements Serializable {


  private static final long serialVersionUID = 1L;

  public enum TableInfoType {
    INPUT_INFO,
    OUTPUT_INFO
  };

  private final TableInfoType tableInfoType;

  /** The Metadata server uri */
  private final String serverUri;

  /** If the howl server is configured to work with hadoop security, this
   * variable will hold the principal name of the server - this will be used
   * in the authentication to the howl server using kerberos
   */
  private final String serverKerberosPrincipal;

  /** The db and table names */
  private final String dbName;
  private final String tableName;

  /** The partition filter */
  private String filter;

  /** The partition predicates to filter on, an arbitrary AND/OR filter, if used to input from*/
  private final String partitionPredicates;

  /** The information about the partitions matching the specified query */
  private JobInfo jobInfo;

  /** The partition values to publish to, if used for output*/
  private Map<String, String> partitionValues;

  /**
   * Initializes a new HowlTableInfo instance to be used with {@link HowlInputFormat}
   * for reading data from a table.
   * @param serverUri the Metadata server uri
   * @param serverKerberosPrincipal If the howl server is configured to
   * work with hadoop security, the kerberos principal name of the server - else null
   * The principal name should be of the form:
   * <servicename>/_HOST@<realm> like "howl/_HOST@myrealm.com"
   * The special string _HOST will be replaced automatically with the correct host name
   * @param dbName the db name
   * @param tableName the table name
   */
  public static HowlTableInfo getInputTableInfo(String serverUri,
      String serverKerberosPrincipal,
      String dbName,
          String tableName) {
    return new HowlTableInfo(serverUri, serverKerberosPrincipal, dbName, tableName, (String) null);
  }

  /**
   * Initializes a new HowlTableInfo instance to be used with {@link HowlInputFormat}
   * for reading data from a table.
   * @param serverUri the Metadata server uri
   * @param serverKerberosPrincipal If the howl server is configured to
   * work with hadoop security, the kerberos principal name of the server - else null
   * The principal name should be of the form:
   * <servicename>/_HOST@<realm> like "howl/_HOST@myrealm.com"
   * The special string _HOST will be replaced automatically with the correct host name
   * @param dbName the db name
   * @param tableName the table name
   * @param filter the partition filter
   */
  public static HowlTableInfo getInputTableInfo(String serverUri, String serverKerberosPrincipal, String dbName,
          String tableName, String filter) {
    return new HowlTableInfo(serverUri, serverKerberosPrincipal, dbName, tableName, filter);
  }

  private HowlTableInfo(String serverUri, String serverKerberosPrincipal,
      String dbName, String tableName, String filter) {
      this.serverUri = serverUri;
      this.serverKerberosPrincipal = serverKerberosPrincipal;
      this.dbName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;
      this.tableName = tableName;
      this.partitionPredicates = null;
      this.partitionValues = null;
      this.tableInfoType = TableInfoType.INPUT_INFO;
      this.filter = filter;
  }
  /**
   * Initializes a new HowlTableInfo instance to be used with {@link HowlOutputFormat}
   * for writing data from a table.
   * @param serverUri the Metadata server uri
   * @param serverKerberosPrincipal If the howl server is configured to
   * work with hadoop security, the kerberos principal name of the server - else null
   * The principal name should be of the form:
   * <servicename>/_HOST@<realm> like "howl/_HOST@myrealm.com"
   * The special string _HOST will be replaced automatically with the correct host name
   * @param dbName the db name
   * @param tableName the table name
   * @param partitionValues The partition values to publish to, can be null or empty Map to
   * indicate write to a unpartitioned table. For partitioned tables, this map should
   * contain keys for all partition columns with corresponding values.
   */
  public static HowlTableInfo getOutputTableInfo(String serverUri,
          String serverKerberosPrincipal, String dbName, String tableName, Map<String, String> partitionValues){
      return new HowlTableInfo(serverUri, serverKerberosPrincipal, dbName,
          tableName, partitionValues);
  }

  private HowlTableInfo(String serverUri, String serverKerberosPrincipal,
      String dbName, String tableName, Map<String, String> partitionValues){
    this.serverUri = serverUri;
    this.serverKerberosPrincipal = serverKerberosPrincipal;
    this.dbName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;
    this.tableName = tableName;
    this.partitionPredicates = null;
    this.partitionValues = partitionValues;
    this.tableInfoType = TableInfoType.OUTPUT_INFO;
  }

  /**
   * Gets the value of serverUri
   * @return the serverUri
   */
  public String getServerUri() {
    return serverUri;
  }

  /**
   * Gets the value of dbName
   * @return the dbName
   */
  public String getDatabaseName() {
    return dbName;
  }

  /**
   * Gets the value of tableName
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the value of partitionPredicates
   * @return the partitionPredicates
   */
  public String getPartitionPredicates() {
    return partitionPredicates;
  }

  /**
   * Gets the value of partitionValues
   * @return the partitionValues
   */
  public Map<String, String> getPartitionValues() {
    return partitionValues;
  }

  /**
   * Gets the value of job info
   * @return the job info
   */
  public JobInfo getJobInfo() {
    return jobInfo;
  }

  /**
   * Sets the value of jobInfo
   * @param jobInfo the jobInfo to set
   */
  public void setJobInfo(JobInfo jobInfo) {
    this.jobInfo = jobInfo;
  }

  public TableInfoType getTableType(){
    return this.tableInfoType;
  }

  /**
   * Sets the value of partitionValues
   * @param partitionValues the partition values to set
   */
  void setPartitionValues(Map<String, String>  partitionValues) {
    this.partitionValues = partitionValues;
  }

  /**
   * Gets the value of partition filter
   * @return the filter string
   */
  public String getFilter() {
    return filter;
  }

  /**
   * @return the serverKerberosPrincipal
   */
  public String getServerKerberosPrincipal() {
    return serverKerberosPrincipal;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31*result + (serverUri == null ? 0 : serverUri.hashCode());
    result = 31*result + (serverKerberosPrincipal == null ? 0 : serverKerberosPrincipal.hashCode());
    result = 31*result + (dbName == null? 0 : dbName.hashCode());
    result = 31*result + tableName.hashCode();
    result = 31*result + (filter == null? 0 : filter.hashCode());
    result = 31*result + (partitionPredicates == null ? 0 : partitionPredicates.hashCode());
    result = 31*result + tableInfoType.ordinal();
    result = 31*result + (partitionValues == null ? 0 : partitionValues.hashCode());
    return result;

  }
}

