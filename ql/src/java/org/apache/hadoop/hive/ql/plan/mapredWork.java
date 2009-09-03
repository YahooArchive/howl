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

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName="Map Reduce")
public class mapredWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private String command;
  // map side work
  //   use LinkedHashMap to make sure the iteration order is
  //   deterministic, to ease testing
  private LinkedHashMap<String,ArrayList<String>> pathToAliases;
  
  private LinkedHashMap<String,partitionDesc> pathToPartitionInfo;
  
  private LinkedHashMap<String,Operator<? extends Serializable>> aliasToWork;
  
  private LinkedHashMap<String, partitionDesc> aliasToPartnInfo;

  // map<->reduce interface
  // schema of the map-reduce 'key' object - this is homogeneous
  private tableDesc keyDesc;

  // schema of the map-reduce 'val' object - this is heterogeneous
  private List<tableDesc> tagToValueDesc;

  private Operator<?> reducer;
  
  private Integer numReduceTasks;
  
  private boolean needsTagging;
  private mapredLocalWork mapLocalWork;

  public mapredWork() { 
    this.aliasToPartnInfo = new LinkedHashMap<String, partitionDesc>();
  }

  public mapredWork(
    final String command,
    final LinkedHashMap<String,ArrayList<String>> pathToAliases,
    final LinkedHashMap<String,partitionDesc> pathToPartitionInfo,
    final LinkedHashMap<String,Operator<? extends Serializable>> aliasToWork,
    final tableDesc keyDesc,
    List<tableDesc> tagToValueDesc,
    final Operator<?> reducer,
    final Integer numReduceTasks,
    final mapredLocalWork mapLocalWork) {
      this.command = command;
      this.pathToAliases = pathToAliases;
      this.pathToPartitionInfo = pathToPartitionInfo;
      this.aliasToWork = aliasToWork;
      this.keyDesc = keyDesc;
      this.tagToValueDesc = tagToValueDesc;
      this.reducer = reducer;
      this.numReduceTasks = numReduceTasks;
      this.mapLocalWork = mapLocalWork;
      this.aliasToPartnInfo = new LinkedHashMap<String, partitionDesc>();
  }

  public String getCommand() {
    return this.command;
  }
  public void setCommand(final String command) {
    this.command = command;
  }

  @explain(displayName="Path -> Alias", normalExplain=false)
  public LinkedHashMap<String,ArrayList<String>> getPathToAliases() {
    return this.pathToAliases;
  }
  public void setPathToAliases(final LinkedHashMap<String,ArrayList<String>> pathToAliases) {
    this.pathToAliases = pathToAliases;
  }

  @explain(displayName="Path -> Partition", normalExplain=false)
  public LinkedHashMap<String,partitionDesc> getPathToPartitionInfo() {
    return this.pathToPartitionInfo;
  }
  public void setPathToPartitionInfo(final LinkedHashMap<String,partitionDesc> pathToPartitionInfo) {
    this.pathToPartitionInfo = pathToPartitionInfo;
  }
  
  /**
   * @return the aliasToPartnInfo
   */
  public LinkedHashMap<String, partitionDesc> getAliasToPartnInfo() {
    return aliasToPartnInfo;
  }
  
  /**
   * @param aliasToPartnInfo the aliasToPartnInfo to set
   */
  public void setAliasToPartnInfo(
      LinkedHashMap<String, partitionDesc> aliasToPartnInfo) {
    this.aliasToPartnInfo = aliasToPartnInfo;
  }
  
  @explain(displayName="Alias -> Map Operator Tree")
  public LinkedHashMap<String, Operator<? extends Serializable>> getAliasToWork() {
    return this.aliasToWork;
  }
  public void setAliasToWork(final LinkedHashMap<String,Operator<? extends Serializable>> aliasToWork) {
    this.aliasToWork=aliasToWork;
  }


  /**
   * @return the mapredLocalWork
   */
  @explain(displayName="Local Work")
  public mapredLocalWork getMapLocalWork() {
    return mapLocalWork;
  }

  /**
   * @param mapLocalWork the mapredLocalWork to set
   */
  public void setMapLocalWork(final mapredLocalWork mapLocalWork) {
    this.mapLocalWork = mapLocalWork;
  }

  public tableDesc getKeyDesc() {
    return this.keyDesc;
  }
  public void setKeyDesc(final tableDesc keyDesc) {
    this.keyDesc = keyDesc;
  }
  public List<tableDesc> getTagToValueDesc() {
    return tagToValueDesc;
  }
  public void setTagToValueDesc(final List<tableDesc> tagToValueDesc) {
    this.tagToValueDesc = tagToValueDesc;
  }

  @explain(displayName="Reduce Operator Tree")
  public Operator<?> getReducer() {
    return this.reducer;
  }

  public void setReducer(final Operator<?> reducer) {
    this.reducer = reducer;
  }

  /**
   * If the number of reducers is -1, the runtime will automatically 
   * figure it out by input data size.
   * 
   * The number of reducers will be a positive number only in case the
   * target table is bucketed into N buckets (through CREATE TABLE).
   * This feature is not supported yet, so the number of reducers will 
   * always be -1 for now.
   */
  public Integer getNumReduceTasks() {
    return this.numReduceTasks;
  }
  public void setNumReduceTasks(final Integer numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }
  @SuppressWarnings("nls")
  public void  addMapWork(String path, String alias, Operator<?> work, partitionDesc pd) {
    ArrayList<String> curAliases = this.pathToAliases.get(path);
    if(curAliases == null) {
      assert(this.pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<String> ();
      this.pathToAliases.put(path, curAliases);
      this.pathToPartitionInfo.put(path, pd);
    } else {
      assert(this.pathToPartitionInfo.get(path) != null);
    }

    for(String oneAlias: curAliases) {
      if(oneAlias.equals(alias)) {
        throw new RuntimeException ("Multiple aliases named: " + alias + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if(this.aliasToWork.get(alias) != null) {
      throw new RuntimeException ("Existing work for alias: " + alias);
    }
    this.aliasToWork.put(alias, work);
  }

  @SuppressWarnings("nls")
  public String isInvalid () {
    if((getNumReduceTasks() >= 1) && (getReducer() == null)) {
      return "Reducers > 0 but no reduce operator";
    }

    if((getNumReduceTasks() == 0) && (getReducer() != null)) {
      return "Reducers == 0 but reduce operator specified";
    }

    return null;
  }

  public String toXML () {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Utilities.serializeMapRedWork(this, baos);
    return (baos.toString());
  }

  // non bean

  /**
   * For each map side operator - stores the alias the operator is working on behalf
   * of in the operator runtime state. This is used by reducesink operator - but could
   * be useful for debugging as well.
   */
  private void setAliases () {
    for(String oneAlias: this.aliasToWork.keySet()) {
      this.aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  public void initialize () {
    setAliases();
  }

  @explain(displayName="Needs Tagging", normalExplain=false)
  public boolean getNeedsTagging() {
    return this.needsTagging;
  }
  
  public void setNeedsTagging(boolean needsTagging) {
    this.needsTagging = needsTagging;
  }

}
