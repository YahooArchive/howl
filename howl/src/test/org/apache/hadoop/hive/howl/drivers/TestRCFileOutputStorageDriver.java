/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hive.howl.drivers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.HowlUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

public class TestRCFileOutputStorageDriver extends TestCase {

  public void testConversion() throws IOException {
    Configuration conf = new Configuration();
    JobContext jc = new JobContext(conf, new JobID());

    HowlSchema schema = buildHiveSchema();
    HowlInputStorageDriver isd = new RCFileInputStorageDriver();

    isd.setOriginalSchema(jc, schema);
    isd.setOutputSchema(jc, schema);
    isd.initialize(jc, new Properties());

    byte[][] byteArray = buildBytesArray();

    BytesRefArrayWritable bytesWritable = new BytesRefArrayWritable(byteArray.length);
    for (int i = 0; i < byteArray.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(byteArray[i], 0, byteArray[i].length);
      bytesWritable.set(i, cu);
    }

    //Convert byte array to HowlRecord using isd, convert howlrecord back to byte array
    //using osd, compare the two arrays
    HowlRecord record = isd.convertToHowlRecord(null, bytesWritable);

    HowlOutputStorageDriver osd = new RCFileOutputStorageDriver();

    osd.setSchema(jc, schema);
    osd.initialize(jc, new Properties());

    BytesRefArrayWritable bytesWritableOutput = (BytesRefArrayWritable) osd.convertValue(record);

    assertTrue(bytesWritableOutput.compareTo(bytesWritable) == 0);
  }

  private byte[][] buildBytesArray() throws UnsupportedEncodingException {
    byte[][] bytes = {"123".getBytes("UTF-8"), "456".getBytes("UTF-8"),
        "789".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
        "5.3".getBytes("UTF-8"), "howl and hadoop".getBytes("UTF-8"),
        new byte[0], "NULL".getBytes("UTF-8") };
    return bytes;
  }

  private HowlSchema buildHiveSchema(){

    List<FieldSchema> fields = new ArrayList<FieldSchema>(8);
    fields.add(new FieldSchema("atinyint", "tinyint", ""));
    fields.add(new FieldSchema("asmallint", "smallint", ""));
    fields.add(new FieldSchema("aint", "int", ""));
    fields.add(new FieldSchema("along", "bigint", ""));
    fields.add(new FieldSchema("adouble", "double", ""));
    fields.add(new FieldSchema("astring", "string", ""));
    fields.add(new FieldSchema("anullint", "int", ""));
    fields.add(new FieldSchema("anullstring", "string", ""));

    return new HowlSchema(HowlUtil.getHowlFieldSchemaList(fields));
  }
}
