/*
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.JobContext;

public class HowlUtil {

  public static boolean checkJobContextIfRunningFromBackend(JobContext j){
    if (j.getConfiguration().get("mapred.task.id", "").equals("")){
      return false;
    }
    return true;
  }

  public static String serialize(Serializable obj) throws IOException {
    if (obj == null) {
      return "";
    }
    try {
      ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
      ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
      objStream.writeObject(obj);
      objStream.close();
      return encodeBytes(serialObj.toByteArray());
    } catch (Exception e) {
      throw new IOException("Serialization error: " + e.getMessage(), e);
    }
  }

  public static Object deserialize(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return null;
    }
    try {
      ByteArrayInputStream serialObj = new ByteArrayInputStream(decodeBytes(str));
      ObjectInputStream objStream = new ObjectInputStream(serialObj);
      return objStream.readObject();
    } catch (Exception e) {
      throw new IOException("Deserialization error: " + e.getMessage(), e);
    }
  }

  public static String encodeBytes(byte[] bytes) {
    StringBuffer strBuf = new StringBuffer();

    for (int i = 0; i < bytes.length; i++) {
      strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ('a')));
      strBuf.append((char) (((bytes[i]) & 0xF) + ('a')));
    }

    return strBuf.toString();
  }

  public static byte[] decodeBytes(String str) {
    byte[] bytes = new byte[str.length() / 2];
    for (int i = 0; i < str.length(); i+=2) {
      char c = str.charAt(i);
      bytes[i/2] = (byte) ((c - 'a') << 4);
      c = str.charAt(i+1);
      bytes[i/2] += (c - 'a');
    }
    return bytes;
  }

  public static List<HowlFieldSchema> getHowlFieldSchemaList(List<FieldSchema> fields) {
      if(fields == null) {
          return null;
      } else {
          List<HowlFieldSchema> result = new ArrayList<HowlFieldSchema>();
          for(FieldSchema f: fields) {
              result.add(new HowlFieldSchema(f));
          }
          return result;
      }
  }

  public static HowlSchema extractSchemaFromStorageDescriptor(StorageDescriptor sd) throws IOException {
    if (sd == null){
      throw new IOException("Cannot construct partition info from an empty storage descriptor.");
    }
    HowlSchema schema = new HowlSchema(HowlUtil.getHowlFieldSchemaList(sd.getCols()));
    return schema;
  }

  public static List<FieldSchema> getFieldSchemaList(List<HowlFieldSchema> howlFields) {
      if(howlFields == null) {
          return null;
      } else {
          List<FieldSchema> result = new ArrayList<FieldSchema>();
          for(FieldSchema f: howlFields) {
              result.add(f);
          }
          return result;
      }
  }


  public static Table getTable(HiveMetaStoreClient client, String dbName, String tableName) throws Exception{
    return client.getTable(dbName,tableName);
  }

  public static HowlSchema getTableSchemaWithPtnCols(Table table) throws IOException{
    HowlSchema tableSchema = extractSchemaFromStorageDescriptor(table.getSd());

    if( table.getPartitionKeys().size() != 0 ) {

      // add partition keys to table schema
      // NOTE : this assumes that we do not ever have ptn keys as columns inside the table schema as well!
      for (FieldSchema fs : table.getPartitionKeys()){
          tableSchema.getHowlFieldSchemas().add(new HowlFieldSchema(fs));
      }
    }
    return tableSchema;
  }

}
