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

package org.apache.hadoop.hive.howl.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

public class TestHowlOutputFormat extends TestCase {
  private HiveMetaStoreClient client;
  private HiveConf hiveConf;

  private static final String dbName = "howlOutputFormatTestDB";
  private static final String tblName = "howlOutputFormatTestTable";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());

    try {
      client = new HiveMetaStoreClient(hiveConf, null);

      initTable();
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);

      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private void initTable() throws Exception {

    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
    boolean ret = client.createDatabase(dbName, "howlTest_loc");
    assertTrue("Unable to create the database " + dbName, ret);

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("colname", Constants.STRING_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(fields);
    tbl.setSd(sd);

    //sd.setLocation("hdfs://tmp");
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(
        org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    tbl.setPartitionKeys(fields);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put(InitializeInput.HOWL_OSD_CLASS, OutputSDTest.class.getName());
    tableParams.put(InitializeInput.HOWL_ISD_CLASS, "testInputClass");
    tableParams.put("howl.testarg", "testArgValue");

    tbl.setParameters(tableParams);

    client.createTable(tbl);
  }

  public void testSetOutput() throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "test outputformat");

    Map<String, String> partitionValues = new HashMap<String, String>();
    partitionValues.put("colname", "p1");
    //null server url means local mode
    HowlTableInfo info = HowlTableInfo.getOutputTableInfo(null, dbName, tblName, partitionValues);

    HowlOutputFormat.setOutput(job, info);
    OutputJobInfo jobInfo = HowlOutputFormat.getJobInfo(job);

    assertNotNull(jobInfo.getTableInfo());
    assertEquals(1, jobInfo.getTableInfo().getPartitionValues().size());
    assertEquals("p1", jobInfo.getTableInfo().getPartitionValues().get("colname"));
    assertEquals(1, jobInfo.getTableSchema().getHowlFieldSchemas().size());
    assertEquals("colname", jobInfo.getTableSchema().getHowlFieldSchemas().get(0).getName());

    StorerInfo storer = jobInfo.getStorerInfo();
    assertEquals(OutputSDTest.class.getName(), storer.getOutputSDClass());

    publishTest(job);
  }

  public void publishTest(Job job) throws Exception {
    HowlOutputCommitter committer = new HowlOutputCommitter(null);
    committer.cleanupJob(job);

    Partition part = client.getPartition(dbName, tblName, Arrays.asList("p1"));
    assertNotNull(part);

    StorerInfo storer = InitializeInput.extractStorerInfo(part.getParameters());
    assertEquals(storer.getInputSDClass(), "testInputClass");
    assertEquals(storer.getProperties().get("howl.testarg"), "testArgValue");
    assertTrue(part.getSd().getLocation().indexOf("colname=p1") != -1);
  }
}