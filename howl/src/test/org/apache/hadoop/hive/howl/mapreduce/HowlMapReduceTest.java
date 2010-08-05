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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver;
import org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Test for HowlOutputFormat. Writes a partition using HowlOutputFormat and reads
 * it back using HowlInputFormat, checks the column values and counts.
 */
public abstract class HowlMapReduceTest extends TestCase {

  protected String dbName = "default";
  protected String tableName = "testHowlMapReduceTable";

  protected String inputFormat = RCFileInputFormat.class.getName();
  protected String outputFormat = RCFileOutputFormat.class.getName();
  protected String inputSD = RCFileInputStorageDriver.class.getName();
  protected String outputSD = RCFileOutputStorageDriver.class.getName();
  protected String serdeClass = ColumnarSerDe.class.getName();

  private static List<HowlRecord> writeRecords = new ArrayList<HowlRecord>();
  private static List<HowlRecord> readRecords = new ArrayList<HowlRecord>();

  protected abstract void initialize() throws Exception;

  protected abstract List<FieldSchema> getPartitionKeys();

  protected abstract List<FieldSchema> getTableColumns();

  private HiveMetaStoreClient client;
  protected HiveConf hiveConf;

  private FileSystem fs;
  private String thriftUri = null;

  protected Driver driver;

  @Override
  protected void setUp() throws Exception {
    hiveConf = new HiveConf(this.getClass());

    //The default org.apache.hadoop.hive.ql.hooks.PreExecutePrinter hook
    //is present only in the ql/test directory
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    driver = new Driver(hiveConf);

    thriftUri = System.getenv("HOWL_METASTORE_URI");

    if( thriftUri != null ) {
      System.out.println("Using URI " + thriftUri);

      hiveConf.set("hive.metastore.local", "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
    }

    fs = new LocalFileSystem();
    fs.initialize(fs.getWorkingDirectory().toUri(), new Configuration());

    initialize();

    client = new HiveMetaStoreClient(hiveConf, null);
    initTable();
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

      client.dropTable(databaseName, tableName);
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }

    client.close();
  }



  private void initTable() throws Exception {

    String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

    try {
      client.dropTable(databaseName, tableName);
    } catch(Exception e) {
    } //can fail with NoSuchObjectException


    Table tbl = new Table();
    tbl.setDbName(databaseName);
    tbl.setTableName(tableName);
    tbl.setTableType("MANAGED_TABLE");
    StorageDescriptor sd = new StorageDescriptor();

    sd.setCols(getTableColumns());
    tbl.setPartitionKeys(getPartitionKeys());

    tbl.setSd(sd);

    sd.setBucketCols(new ArrayList<String>(2));
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(serdeClass);
    sd.setInputFormat(inputFormat);
    sd.setOutputFormat(outputFormat);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put(InitializeInput.HOWL_ISD_CLASS, inputSD);
    tableParams.put(InitializeInput.HOWL_OSD_CLASS, outputSD);
    tbl.setParameters(tableParams);

    client.createTable(tbl);
  }

  //Create test input file with specified number of rows
  private void createInputFile(Path path, int rowCount) throws IOException {

    if( fs.exists(path) ) {
      fs.delete(path, true);
    }

    FSDataOutputStream os = fs.create(path);

    for(int i = 0;i < rowCount;i++) {
      os.writeChars(i + "\n");
    }

    os.close();
  }

  public static class MapCreate extends
  Mapper<LongWritable, Text, BytesWritable, HowlRecord> {

    static int writeCount = 0; //test will be in local mode

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      {
        try {
          HowlRecord rec = writeRecords.get(writeCount);

          context.write(null, rec);
          //System.out.println("Wrote row " + writeCount);
          writeCount++;
        }catch(Exception e) {
          e.printStackTrace(); //print since otherwise exception is lost
          throw new IOException(e);
        }
      }
    }
  }

  public static class MapRead extends
  Mapper<WritableComparable, HowlRecord, BytesWritable, Text> {

    static int readCount = 0; //test will be in local mode

    @Override
    public void map(WritableComparable key, HowlRecord value, Context context
    ) throws IOException, InterruptedException {
      {
        try {
          readRecords.add(value);
          readCount++;
        } catch(Exception e) {
          e.printStackTrace(); //print since otherwise exception is lost
          throw new IOException(e);
        }
      }
    }
  }

  void runMRCreate(Map<String, String> partitionValues,
        List<HowlFieldSchema> partitionColumns, List<HowlRecord> records,
        int writeCount) throws Exception {

    writeRecords = records;
    MapCreate.writeCount = 0;

    Configuration conf = new Configuration();
    Job job = new Job(conf, "howl mapreduce write test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HowlMapReduceTest.MapCreate.class);

    // input/output settings
    job.setInputFormatClass(TextInputFormat.class);

    Path path = new Path(fs.getWorkingDirectory(), "mapred/testHowlMapReduceInput");
    createInputFile(path, writeCount);

    TextInputFormat.setInputPaths(job, path);

    job.setOutputFormatClass(HowlOutputFormat.class);

    HowlTableInfo outputInfo = HowlTableInfo.getOutputTableInfo(thriftUri, dbName, tableName, partitionValues);
    HowlOutputFormat.setOutput(job, outputInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(DefaultHowlRecord.class);

    job.setNumReduceTasks(0);

    HowlOutputFormat.setSchema(job, new HowlSchema(partitionColumns));

    //new HowlOutputCommitter(null).setupJob(job);
    job.waitForCompletion(true);
    new HowlOutputCommitter(null).cleanupJob(job);

    Assert.assertEquals(writeCount, MapCreate.writeCount);
  }


  List<HowlRecord> runMRRead(int readCount) throws Exception {

    MapRead.readCount = 0;
    readRecords.clear();

    Configuration conf = new Configuration();
    Job job = new Job(conf, "howl mapreduce read test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HowlMapReduceTest.MapRead.class);

    // input/output settings
    job.setInputFormatClass(HowlInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HowlTableInfo inputInfo = HowlTableInfo.getInputTableInfo(thriftUri, dbName, tableName);
    HowlInputFormat.setInput(job, inputInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    Path path = new Path(fs.getWorkingDirectory(), "mapred/testHowlMapReduceOutput");
    if( fs.exists(path) ) {
      fs.delete(path, true);
    }

    TextOutputFormat.setOutputPath(job, path);

    job.waitForCompletion(true);
    Assert.assertEquals(readCount, MapRead.readCount);

    return readRecords;
  }


  protected HowlSchema getTableSchema() throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf, "howl mapreduce read schema test");
    job.setJarByClass(this.getClass());

    // input/output settings
    job.setInputFormatClass(HowlInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HowlTableInfo inputInfo = HowlTableInfo.getInputTableInfo(thriftUri, dbName, tableName);
    HowlInputFormat.setInput(job, inputInfo);

    return HowlInputFormat.getTableSchema(job);
  }

}



