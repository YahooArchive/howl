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

import org.apache.hadoop.hive.howl.common.ErrorType;
import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchemaUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;

public class TestHowlPartitioned extends HowlMapReduceTest {

  private List<HowlRecord> writeRecords;
  private List<HowlFieldSchema> partitionColumns;

  @Override
  protected void initialize() throws Exception {

    tableName = "testHowlPartitionedTable";
    writeRecords = new ArrayList<HowlRecord>();

    for(int i = 0;i < 20;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      writeRecords.add(new DefaultHowlRecord(objList));
    }

    partitionColumns = new ArrayList<HowlFieldSchema>();
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c2", Constants.STRING_TYPE_NAME, "")));
  }


  @Override
  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("PaRT1", Constants.STRING_TYPE_NAME, ""));
    return fields;
  }

  @Override
  protected List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("c1", Constants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c2", Constants.STRING_TYPE_NAME, ""));
    return fields;
  }


  public void testHowlPartitionedTable() throws Exception {

    Map<String, String> partitionMap = new HashMap<String, String>();
    partitionMap.put("part1", "p1value1");

    runMRCreate(partitionMap, partitionColumns, writeRecords, 10);

    partitionMap.clear();
    partitionMap.put("PART1", "p1value2");

    runMRCreate(partitionMap, partitionColumns, writeRecords, 20);

    //Test for duplicate publish
    IOException exc = null;
    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_DUPLICATE_PARTITION, ((HowlException) exc).getErrorType());

    //Test for publish with invalid partition key name
    exc = null;
    partitionMap.clear();
    partitionMap.put("px", "p1value2");

    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_MISSING_PARTITION_KEY, ((HowlException) exc).getErrorType());


    //Test for null partition value map
    exc = null;
    try {
      runMRCreate(null, partitionColumns, writeRecords, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_INVALID_PARTITION_VALUES, ((HowlException) exc).getErrorType());

    //Read should get 10 + 20 rows
    runMRRead(30);

    //Read with partition filter
    runMRRead(10, "part1 = \"p1value1\"");
    runMRRead(20, "part1 = \"p1value2\"");
    runMRRead(30, "part1 = \"p1value1\" or part1 = \"p1value2\"");

    tableSchemaTest();
    columnOrderChangeTest();
    hiveReadTest();
  }


  //test that new columns gets added to table schema
  private void tableSchemaTest() throws Exception {

    HowlSchema tableSchema = getTableSchema();

    assertEquals(3, tableSchema.getFields().size());

    //Update partition schema to have 3 fields
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c3", Constants.STRING_TYPE_NAME, "")));

    writeRecords = new ArrayList<HowlRecord>();

    for(int i = 0;i < 20;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      objList.add("str2value" + i);

      writeRecords.add(new DefaultHowlRecord(objList));
    }

    Map<String, String> partitionMap = new HashMap<String, String>();
    partitionMap.put("part1", "p1value5");

    runMRCreate(partitionMap, partitionColumns, writeRecords, 10);

    tableSchema = getTableSchema();

    //assert that c3 has got added to table schema
    assertEquals(4, tableSchema.getFields().size());
    assertEquals("c1", tableSchema.getFields().get(0).getName());
    assertEquals("c2", tableSchema.getFields().get(1).getName());
    assertEquals("c3", tableSchema.getFields().get(2).getName());
    assertEquals("part1", tableSchema.getFields().get(3).getName());

    //Test that changing column data type fails
    partitionMap.clear();
    partitionMap.put("part1", "p1value6");

    partitionColumns = new ArrayList<HowlFieldSchema>();
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c2", Constants.INT_TYPE_NAME, "")));

    IOException exc = null;
    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, ((HowlException) exc).getErrorType());

    //Test that partition key is not allowed in data
    partitionColumns = new ArrayList<HowlFieldSchema>();
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c2", Constants.STRING_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c3", Constants.STRING_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("part1", Constants.INT_TYPE_NAME, "")));

    exc = null;
    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_SCHEMA_PARTITION_KEY, ((HowlException) exc).getErrorType());
  }

  //check behavior while change the order of columns
  private void columnOrderChangeTest() throws Exception {

    HowlSchema tableSchema = getTableSchema();

    assertEquals(4, tableSchema.getFields().size());

    partitionColumns = new ArrayList<HowlFieldSchema>();
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c3", Constants.STRING_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c2", Constants.STRING_TYPE_NAME, "")));


    writeRecords = new ArrayList<HowlRecord>();

    for(int i = 0;i < 10;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("co strvalue" + i);
      objList.add("co str2value" + i);

      writeRecords.add(new DefaultHowlRecord(objList));
    }

    Map<String, String> partitionMap = new HashMap<String, String>();
    partitionMap.put("part1", "p1value8");


    Exception exc = null;
    try {
      runMRCreate(partitionMap, partitionColumns, writeRecords, 10);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc instanceof HowlException);
    assertEquals(ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH, ((HowlException) exc).getErrorType());


    partitionColumns = new ArrayList<HowlFieldSchema>();
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c1", Constants.INT_TYPE_NAME, "")));
    partitionColumns.add(HowlSchemaUtils.getHowlFieldSchema(new FieldSchema("c2", Constants.STRING_TYPE_NAME, "")));

    writeRecords = new ArrayList<HowlRecord>();

    for(int i = 0;i < 10;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("co strvalue" + i);

      writeRecords.add(new DefaultHowlRecord(objList));
    }

    runMRCreate(partitionMap, partitionColumns, writeRecords, 10);

    //Read should get 10 + 20 + 10 + 10 rows
    runMRRead(50);
  }

  //Test that data inserted through howloutputformat is readable from hive
  private void hiveReadTest() throws Exception {

    String query = "select * from " + tableName;
    int retCode = driver.run(query).getResponseCode();

    if( retCode != 0 ) {
      throw new Exception("Error " + retCode + " running query " + query);
    }

    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(50, res.size());
  }
}
