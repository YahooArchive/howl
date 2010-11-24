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

package org.apache.hadoop.hive.howl.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchemaUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
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

  public static List<HowlFieldSchema> getHowlFieldSchemaList(List<FieldSchema> fields) throws HowlException {
      if(fields == null) {
          return null;
      } else {
          List<HowlFieldSchema> result = new ArrayList<HowlFieldSchema>();
          for(FieldSchema f: fields) {
              result.add(HowlSchemaUtils.getHowlFieldSchema(f));
          }
          return result;
      }
  }


  public static HowlSchema extractSchemaFromStorageDescriptor(StorageDescriptor sd) throws HowlException {
      if (sd == null){
          throw new HowlException("Cannot construct partition info from an empty storage descriptor.");
        }
        HowlSchema schema = new HowlSchema(HowlUtil.getHowlFieldSchemaList(sd.getCols()));
        return schema;
  }

  public static List<FieldSchema> getFieldSchemaList(List<HowlFieldSchema> howlFields) {
      if(howlFields == null) {
          return null;
      } else {
          List<FieldSchema> result = new ArrayList<FieldSchema>();
          for(HowlFieldSchema f: howlFields) {
              result.add(HowlSchemaUtils.getFieldSchema(f));
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
            tableSchema.append(HowlSchemaUtils.getHowlFieldSchema(fs));
        }
      }
      return tableSchema;
    }

  /**
   * Validate partition schema, checks if the column types match between the partition
   * and the existing table schema. Returns the list of columns present in the partition
   * but not in the table.
   * @param table the table
   * @param partitionSchema the partition schema
   * @return the list of newly added fields
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static List<FieldSchema> validatePartitionSchema(Table table, HowlSchema partitionSchema) throws IOException {
    Map<String, FieldSchema> partitionKeyMap = new HashMap<String, FieldSchema>();

    for(FieldSchema field : table.getPartitionKeys()) {
      partitionKeyMap.put(field.getName().toLowerCase(), field);
    }

    List<FieldSchema> tableCols = table.getSd().getCols();
    List<FieldSchema> newFields = new ArrayList<FieldSchema>();

    for(int i = 0;i <  partitionSchema.getFields().size();i++) {

      FieldSchema field = HowlSchemaUtils.getFieldSchema(partitionSchema.getFields().get(i));

      FieldSchema tableField;
      if( i < tableCols.size() ) {
        tableField = tableCols.get(i);

        if( ! tableField.getName().equalsIgnoreCase(field.getName())) {
          throw new HowlException(ErrorType.ERROR_SCHEMA_COLUMN_MISMATCH, "Expected column <" + tableField.getName() +
              "> at position " + (i + 1) + ", found column <" + field.getName() + ">");
        }
      } else {
        tableField = partitionKeyMap.get(field.getName().toLowerCase());

        if( tableField != null ) {
          throw new HowlException(ErrorType.ERROR_SCHEMA_PARTITION_KEY, "Key <" +  field.getName() + ">");
        }
      }

      if( tableField == null ) {
        //field present in partition but not in table
        newFields.add(field);
      } else {
        //field present in both. validate type has not changed
        TypeInfo partitionType = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
        TypeInfo tableType = TypeInfoUtils.getTypeInfoFromTypeString(tableField.getType());

        if( ! partitionType.equals(tableType) ) {
          throw new HowlException(ErrorType.ERROR_SCHEMA_TYPE_MISMATCH, "Column <" + field.getName() + ">, expected <" +
              tableType.getTypeName() + ">, got <" + partitionType.getTypeName() + ">");
        }
      }
    }

    return newFields;
  }

  /**
   * Test if the first FsAction is more permissive than the second. This is useful in cases where
   * we want to ensure that a file owner has more permissions than the group they belong to, for eg.
   * More completely(but potentially more cryptically)
   *  owner-r >= group-r >= world-r : bitwise and-masked with 0444 => 444 >= 440 >= 400 >= 000
   *  owner-w >= group-w >= world-w : bitwise and-masked with &0222 => 222 >= 220 >= 200 >= 000
   *  owner-x >= group-x >= world-x : bitwise and-masked with &0111 => 111 >= 110 >= 100 >= 000
   * @return true if first FsAction is more permissive than the second, false if not.
   */
  public static boolean validateMorePermissive(FsAction first, FsAction second) {
    if ((first == FsAction.ALL) ||
        (second == FsAction.NONE) ||
        (first == second)) {
      return true;
    }
    switch (first){
      case READ_EXECUTE : return ((second == FsAction.READ) || (second == FsAction.EXECUTE));
      case READ_WRITE : return ((second == FsAction.READ) || (second == FsAction.WRITE));
      case WRITE_EXECUTE : return ((second == FsAction.WRITE) || (second == FsAction.EXECUTE));
    }
    return false;
  }
}
