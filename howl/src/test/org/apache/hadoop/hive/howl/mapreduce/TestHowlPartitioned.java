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

import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;

public class TestHowlPartitioned extends HowlMapReduceTest {

  @Override
  protected void initialize() {

    writeRecords = new ArrayList<HowlRecord>();

    for(int i = 0;i < 20;i++) {
      List<Object> objList = new ArrayList<Object>();

      objList.add(i);
      objList.add("strvalue" + i);
      writeRecords.add(new DefaultHowlRecord(objList));
    }
  }


  @Override
  protected List<HowlFieldSchema> getPartitionColumns() {
    List<HowlFieldSchema> fields = new ArrayList<HowlFieldSchema>();
    fields.add(new HowlFieldSchema("c1", Constants.INT_TYPE_NAME, ""));
    fields.add(new HowlFieldSchema("c2", Constants.STRING_TYPE_NAME, ""));
    return fields;
  }

  @Override
  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("p1", Constants.STRING_TYPE_NAME, ""));
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
    partitionMap.put("p1", "p1value1");

    runMRCreate(partitionMap, 10);

    partitionMap.clear();
    partitionMap.put("p1", "p1value2");

    runMRCreate(partitionMap, 20);

    //Test for duplicate publish
    IOException exc = null;
    try {
      runMRCreate(partitionMap, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc.getMessage().indexOf(
        "Partition already present with given partition key values") != -1);

    //Test for publish with invalid partition key name
    exc = null;
    partitionMap.clear();
    partitionMap.put("px", "p1value2");

    try {
      runMRCreate(partitionMap, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc.getMessage().indexOf(
        "No partition key value provided for key p1") != -1);


    //Test for null partition value map
    exc = null;
    try {
      runMRCreate(null, 20);
    } catch(IOException e) {
      exc = e;
    }

    assertTrue(exc != null);
    assertTrue(exc.getMessage().indexOf(
        "Invalid partition values specified") != -1);

    //Read should get 10 + 20 rows
    runMRRead(30);
  }
}
