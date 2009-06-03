package org.apache.hadoop.hive.ql.history;

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

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.history.HiveHistory.QueryInfo;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.tools.LineageInfo;
import org.apache.hadoop.mapred.TextInputFormat;

public class TestHiveHistory extends TestCase {

  static HiveConf conf;

  static private String tmpdir = "/tmp/" + System.getProperty("user.name")
      + "/";
  static private Path tmppath = new Path(tmpdir);
  static private Hive db;
  static private FileSystem fs;

  /*
   * intialize the tables
   */

  protected void setUp(){
    try {
      conf = new HiveConf(HiveHistory.class);

      fs = FileSystem.get(conf);
      if (fs.exists(tmppath) && !fs.getFileStatus(tmppath).isDir()) {
        throw new RuntimeException(tmpdir + " exists but is not a directory");
      }

      if (!fs.exists(tmppath)) {
        if (!fs.mkdirs(tmppath)) {
          throw new RuntimeException("Could not make scratch directory "
              + tmpdir);
        }
      }

      // copy the test files into hadoop if required.
      int i = 0;
      Path[] hadoopDataFile = new Path[2];
      String[] testFiles = { "kv1.txt", "kv2.txt" };
      String testFileDir = "file://"
          + conf.get("test.data.files").replace('\\', '/').replace("c:", "");
      for (String oneFile : testFiles) {
        Path localDataFile = new Path(testFileDir, oneFile);
        hadoopDataFile[i] = new Path(tmppath, oneFile);
        fs.copyFromLocalFile(false, true, localDataFile, hadoopDataFile[i]);
        i++;
      }

      // load the test files into tables
      i = 0;
      db = Hive.get(conf);
      String[] srctables = { "src", "src2" };
      LinkedList<String> cols = new LinkedList<String>();
      cols.add("key");
      cols.add("value");
      for (String src : srctables) {
        db.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, src, true, true);
        db.createTable(src, cols, null, TextInputFormat.class,
            IgnoreKeyTextOutputFormat.class);
        db.loadTable(hadoopDataFile[i], src, false, null);
        i++;
      }

    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException("Encountered throwable");
    }
}

  /**
   * check history file output for this query.als
   */
  public void testSimpleQuery() {
    LineageInfo lep = new LineageInfo();
    try {

      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before
      // any of the other core hive classes are loaded
      SessionState.initHiveLog4j();

      CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
      ss.in = System.in;
      try {
        ss.out = new PrintStream(System.out, true, "UTF-8");
        ss.err = new PrintStream(System.err, true, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        System.exit(3);
      }

      SessionState.start(ss);

      String cmd = "select a.key from src a";
      Driver d = new Driver();
      int ret = d.run(cmd);
      if (ret != 0) {
        fail("Failed");
      }
      HiveHistoryViewer hv = new HiveHistoryViewer(SessionState.get()
          .getHiveHistory().getHistFileName());
      Map<String, QueryInfo> jobInfoMap = hv.getJobInfoMap();
      Map<String, TaskInfo> taskInfoMap = hv.getTaskInfoMap();
      if (jobInfoMap.size() != 1) {
        fail("jobInfo Map size not 1");
      }

      if (taskInfoMap.size() != 1) {
        fail("jobInfo Map size not 1");
      }


      cmd = (String)jobInfoMap.keySet().toArray()[0];
      QueryInfo ji = jobInfoMap.get(cmd);

      if (!ji.hm.get(Keys.QUERY_NUM_TASKS.name()).equals("1")) {
        fail("Wrong number of tasks");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed");
    }
  }

}
