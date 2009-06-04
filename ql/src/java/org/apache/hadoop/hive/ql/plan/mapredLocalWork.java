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

@explain(displayName="Map Reduce Local Work")
public class mapredLocalWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork;
  private LinkedHashMap<String, fetchWork> aliasToFetchWork;

  public mapredLocalWork() { }

  public mapredLocalWork(final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork,
                         final LinkedHashMap<String, fetchWork> aliasToFetchWork) {
    this.aliasToWork = aliasToWork;
    this.aliasToFetchWork = aliasToFetchWork;
  }

  @explain(displayName="Alias -> Map Local Operator Tree")
  public LinkedHashMap<String, Operator<? extends Serializable>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(final LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  /**
   * @return the aliasToFetchWork
   */
  @explain(displayName="Alias -> Map Local Tables")
  public LinkedHashMap<String, fetchWork> getAliasToFetchWork() {
    return aliasToFetchWork;
  }

  /**
   * @param aliasToFetchWork the aliasToFetchWork to set
   */
  public void setAliasToFetchWork(final LinkedHashMap<String, fetchWork> aliasToFetchWork) {
    this.aliasToFetchWork = aliasToFetchWork;
  }
}
