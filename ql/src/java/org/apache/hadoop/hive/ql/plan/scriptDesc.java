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

import org.apache.hadoop.hive.ql.exec.RecordReader;

@explain(displayName="Transform Operator")
public class scriptDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String scriptCmd;
  // Describe how to deserialize data back from user script
  private tableDesc scriptOutputInfo;
  // Describe how to serialize data out to user script
  private tableDesc scriptInputInfo;
  private Class<? extends RecordReader> outRecordReaderClass;

  public scriptDesc() { }
  public scriptDesc(
    final String scriptCmd,
    final tableDesc scriptInputInfo,
    final tableDesc scriptOutputInfo,
    final Class<? extends RecordReader> outRecordReaderClass) {
    
    this.scriptCmd = scriptCmd;
    this.scriptInputInfo = scriptInputInfo;
    this.scriptOutputInfo = scriptOutputInfo;
    this.outRecordReaderClass = outRecordReaderClass;
  }
  
  @explain(displayName="command")
  public String getScriptCmd() {
    return this.scriptCmd;
  }
  public void setScriptCmd(final String scriptCmd) {
    this.scriptCmd=scriptCmd;
  }
  
  @explain(displayName="output info")
  public tableDesc getScriptOutputInfo() {
    return this.scriptOutputInfo;
  }
  public void setScriptOutputInfo(final tableDesc scriptOutputInfo) {
    this.scriptOutputInfo = scriptOutputInfo;
  }
  public tableDesc getScriptInputInfo() {
    return scriptInputInfo;
  }
  public void setScriptInputInfo(tableDesc scriptInputInfo) {
    this.scriptInputInfo = scriptInputInfo;
  }
  /**
   * @return the outRecordReaderClass
   */
  public Class<? extends RecordReader> getOutRecordReaderClass() {
    return outRecordReaderClass;
  }
  /**
   * @param outRecordReaderClass the outRecordReaderClass to set
   */
  public void setOutRecordReaderClass(
      Class<? extends RecordReader> outRecordReaderClass) {
    this.outRecordReaderClass = outRecordReaderClass;
  }
}
