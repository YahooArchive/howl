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


public class TableInputInfo {


  /** The Metadata server uri */
  private final String serverUri;

  /** The db and table names */
  private final String dbName;
  private final String tableName;

  /** The partition predicates to filter on, an arbitrary AND/OR filter */
  private final String partitionPredicates;

  /** The information about the partitions matching the specified query */
  private JobInfo jobInfo;

  /**
   * Initializes a new TableInputInfo instance.
   * @param serverUri the Metadata server uri
   * @param tableName the table name
   * @param partitionPredicates the partition predicates to filter on, an arbitrary AND/OR filter.
   */
  public TableInputInfo(String serverUri, String dbName, String tableName,
      String partitionPredicates) {
    this.serverUri = serverUri;
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionPredicates = partitionPredicates;
  }

  /**
   * Creates a new TableInputInfo instance from a uri-representation of information required to instantiate an TableInputInfo
   * @param uri : Uri representing the input information.
   * For eg: http://localhost:4080/howl/?table=dbname.tablename&seq=001
   * would represent that the serverUri is http://localhost:4080/howl ,
   * tableName is formed from database = dbname and table = tablename
   * and filter is "seq=001". The filter would be uri-encoded for spaces, equals, ampersands, etc
   */
  public TableInputInfo(String uri) {
    // initially, trivial implementations which are comma separated.
    // for eg: http://localhost:4080/howl/,dbname,tablename,seq=001
    // FIXME: change to uri parsing

    String[] params = uri.split(",",4);
    this.serverUri = params[0];
    this.dbName = params[1];
    this.tableName = params[2];
    this.partitionPredicates = params[3];
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
}
