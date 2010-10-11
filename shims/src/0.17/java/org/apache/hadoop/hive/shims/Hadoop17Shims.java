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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UnixUserGroupInformation;
import javax.security.auth.login.LoginException;

import java.io.IOException;

/**
 * Implemention of shims against Hadoop 0.17.0.
 */
public class Hadoop17Shims implements HadoopShims {
  public boolean usesJobShell() {
    return true;
  }

  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path)
    throws IOException {
    return false;
  }

  /**
   * No way to get this information in hadoop 17
   */
  public boolean isJobPreparing(RunningJob job) throws IOException {
    return false;
  }

  public void inputFormatValidateInput(InputFormat fmt, JobConf conf)
    throws IOException {
    fmt.validateInput(conf);
  }

  /**
   * workaround for hadoop-17 - jobclient only looks at commandlineconfig.
   */
  public void setTmpFiles(String prop, String files) {
    Configuration conf = JobClient.getCommandLineConfig();
    if (conf != null) {
      conf.set(prop, files);
    }
  }

  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
                                int numDataNodes,
                                boolean format,
                                String[] racks) throws IOException {
    return new MiniDFSShim(new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private MiniDFSCluster cluster;
    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
  }

  /**
   * We define this function here to make the code compatible between
   * hadoop 0.17 and hadoop 0.20.
   *
   * Hive binary that compiled Text.compareTo(Text) with hadoop 0.20 won't
   * work with hadoop 0.17 because in hadoop 0.20, Text.compareTo(Text) is
   * implemented in org.apache.hadoop.io.BinaryComparable, and Java compiler
   * references that class, which is not available in hadoop 0.17.
   */
  public int compareText(Text a, Text b) {
    return a.compareTo(b);
  }

  @Override
  public long getAccessTime(FileStatus file) {
    return -1;
  }

  public HadoopShims.CombineFileInputFormatShim getCombineFileInputFormat() {
    return null;
  }

  public String getInputFormatClassName() {
    return "org.apache.hadoop.hive.ql.io.HiveInputFormat";
  }

  @Override
  public String [] getTaskJobIDs(TaskCompletionEvent t) {
    return null;
  }

  public void setFloatConf(Configuration conf, String varName, float val) {
    conf.set(varName, Float.toString(val));
  }

  public void setNullOutputFormat(JobConf conf) {
    conf.setOutputFormat(NullOutputFormat.class);
  }
  
  @Override
  public int createHadoopArchive(Configuration conf, Path parentDir, Path destDir,
      String archiveName) throws Exception {
    throw new RuntimeException("Not implemented in this Hadoop version");
  }

  @Override
  public UserGroupInformation getUGIForConf(Configuration conf) throws LoginException {
    UserGroupInformation ugi =
      UnixUserGroupInformation.readFromConf(conf, UnixUserGroupInformation.UGI_PROPERTY_NAME);
    if(ugi == null) {
      ugi = UserGroupInformation.login(conf);
    }
    return ugi;
  }
}
