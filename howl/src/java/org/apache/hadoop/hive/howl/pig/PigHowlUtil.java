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
package org.apache.hadoop.hive.howl.pig;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.howl.data.Pair;
import org.apache.hadoop.hive.howl.data.type.HowlType;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfo;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfoUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class PigHowlUtil {

  public static final String HOWL_TABLE_SCHEMA = "howl.table.schema";


  static final int HowlExceptionCode = 4010;

  private final  Map<Pair<String,String>, Table> howlTableCache =
    new HashMap<Pair<String,String>, Table>();

  static public Pair<String, String> getDBTableNames(String location) throws IOException {
    // the location string will be of the form:
    // <database name>.<table name> - parse it and
    // communicate the information to HowlInputFormat

    String[] dbTableNametokens = location.split("\\.");
    if(dbTableNametokens.length != 2) {
      String locationErrMsg = "The input location in load statement " +
      "should be of the form " +
      "<databasename>.<table name>";
      throw new PigException(locationErrMsg, HowlExceptionCode);
    }
    return new Pair<String, String>(dbTableNametokens[0], dbTableNametokens[1]);
  }

  static public String getHowlServerUri() {
    String howlServerUri;
    final String METADATA_URI = "metadata.uri";
    Properties props = UDFContext.getUDFContext().getClientSystemProps();
    howlServerUri = props.getProperty(METADATA_URI);
    //    if(howlServerUri == null) {
    //      String msg = "Please provide uri to the metadata server using" +
    //      " -Dmetadata.uri system property";
    //      throw new PigException(msg, HowlExceptionCode);
    //    }
    return howlServerUri;
  }

  static HiveMetaStoreClient client = null;

  private static HiveMetaStoreClient createHiveMetaClient(String serverUri, Class clazz) throws Exception {
    if (client != null){
      return client;
    }
    HiveConf hiveConf = new HiveConf(clazz);

    if (serverUri != null){
      hiveConf.set("hive.metastore.uris", serverUri);
    }

    return (client = new HiveMetaStoreClient(hiveConf,null));
  }

  //  HowlFieldSchema getHowlColumnSchema( RequiredField rf) throws IOException{
  //    return getHowlColumnSchema(null,rf);
  //  }

  //  HowlFieldSchema getHowlColumnSchema(String columnName, RequiredField rf) throws IOException{
  //    String colName = (columnName != null) ? columnName : rf.getAlias() ;
  //
  //    HowlTypeInfo hti = null;
  //    if (rf.getType() == DataType.UNKNOWN){
  //      hti = HowlTypeInfoUtils.getPrimitiveTypeInfo(String.class);
  //    } else if (rf.getType() == DataType.BAG){
  //      hti = HowlTypeInfoUtils.getListHowlTypeInfo(
  //          HowlTypeInfoUtils.getHowlTypeInfo(
  //              getHowlColumnSchema(rf.getSubFields().get(1)).getType()
  //          ));
  //    } else if (rf.getType() == DataType.TUPLE){
  //      StructBuilder htiBuilder = HowlTypeInfoUtils.getStructHowlTypeInfoBuilder();
  //      for ( RequiredField subfield : rf.getSubFields()){
  //        htiBuilder.addField(subfield.getAlias(), getHowlColumnSchema(rf).getType());
  //      }
  //      hti = htiBuilder.build();
  //    } else if (rf.getType() == DataType.MAP){
  //      hti = HowlTypeInfoUtils.getMapHowlTypeInfoBuilder()
  //              .withKeyType(HowlTypeInfoUtils.getPrimitiveTypeInfo(String.class))
  //              .withValueType(HowlTypeInfoUtils.getPrimitiveTypeInfo( /* determine map value type here*/))
  //              .build();
  //    } else {
  //      // a primitive
  //      hti = HowlTypeInfoUtils.getPrimitiveTypeInfo(/* find primitive type here from rf.getType*/);
  //    }
  //
  //    return new HowlFieldSchema(colName,hti.getTypeString(),null);
  //  }

  HowlSchema getHowlSchema(List<RequiredField> fields, boolean isSubFields,String signature) throws IOException {
    if(fields == null) {
      return null;
    }
    Properties props = UDFContext.getUDFContext().getUDFProperties(
        this.getClass(), new String[] {signature});
    HowlSchema howlTableSchema = (HowlSchema) props.get(HOWL_TABLE_SCHEMA);
    ArrayList<HowlFieldSchema> fcols = new ArrayList<HowlFieldSchema>();
    for(RequiredField rf: fields) {
      //        String colName = null;
      //        if(isSubFields){
      //            // for subfields, use the column alias, not index
      //            colName = rf.getAlias();
      //        }else {
      //            colName = howlTableSchema.getHowlFieldSchemas().get(rf.getIndex()).getName();
      //        }
      //        fcols.add(getHowlColumnSchema(colName = howlTableSchema.getHowlFieldSchemas().get(rf.getIndex()).getName(), rf));
      fcols.add(howlTableSchema.getHowlFieldSchemas().get(rf.getIndex()));
    }
    return new HowlSchema(fcols);
  }

  public Table getTable(String location, String howlServerUri) throws IOException{
    Pair<String, String> loc_server = new Pair(location, howlServerUri);
    Table howlTable = howlTableCache.get(loc_server);
    if(howlTable != null){
      return howlTable;
    }

    Pair<String, String> dbTablePair = PigHowlUtil.getDBTableNames(location);
    String dbName = dbTablePair.first;
    String tableName = dbTablePair.second;
    Table table = null;
    try {
      client = createHiveMetaClient(howlServerUri, PigHowlUtil.class);
      table = client.getTable(dbName, tableName);
    } catch (Exception e) {
      throw new IOException(e);
    }
    howlTableCache.put(loc_server, table);
    return table;
  }

  static public ResourceSchema getResourceSchema(HowlSchema howlSchema) throws IOException {
    if(howlSchema == null) {
      return null;
    }
    //    return getResourceSchema(HowlTypeInfoUtils.getHowlTypeInfo(howlSchema.getHowlFieldSchemas()));
    List<ResourceFieldSchema> rfSchemaList = new ArrayList<ResourceFieldSchema>();
    for (HowlFieldSchema hfs : howlSchema.getHowlFieldSchemas()){
      ResourceFieldSchema rfSchema = new ResourceFieldSchema()
      .setName(hfs.getName())
      .setDescription(hfs.getComment())
      .setType(getPigType( HowlTypeInfoUtils.getHowlTypeInfo(hfs.getType()) ))
      .setSchema(null); // TODO: see if we need to munge inner schemas for these
      rfSchemaList.add(rfSchema);
    }
    ResourceSchema rSchema = new ResourceSchema();
    rSchema.setFields(rfSchemaList.toArray(new ResourceFieldSchema[0]));
    return rSchema;

  }


  /**
   * @param type owl column type
   * @return corresponding pig type
   * @throws IOException
   */
  static public byte getPigType(HowlTypeInfo typeInfo) throws IOException {

    HowlType type = typeInfo.getType();
    return getPigType(type);
  }
  static public byte getPigType(HowlType type) throws IOException {
    String errMsg;

    if (type == HowlType.STRING){
      return DataType.CHARARRAY;
    }

    if ( (type == HowlType.INT) || (type == HowlType.SMALLINT) || (type == HowlType.TINYINT)){
      return DataType.INTEGER;
    }

    if (type == HowlType.ARRAY){
      return DataType.BAG;
    }

    if (type == HowlType.STRUCT){
      return DataType.TUPLE;
    }

    if (type == HowlType.MAP){
      return DataType.MAP;
    }

    if (type == HowlType.BIGINT){
      return DataType.LONG;
    }

    if (type == HowlType.FLOAT){
      return DataType.FLOAT;
    }

    if (type == HowlType.DOUBLE){
      return DataType.DOUBLE;
    }

    if (type == HowlType.BOOLEAN){
      errMsg = "Howl column type 'BOOLEAN' is not supported in " +
      "Pig as a column type";
      throw new IOException(errMsg);
    }

    errMsg = "Howl column type '"+ type.toString() +"' is not supported in Pig as a column type";
    throw new PigException(errMsg, HowlExceptionCode);
  }

  @SuppressWarnings("unchecked")
  public static Tuple transformToTuple(HowlRecord hr) throws Exception{
    if (hr == null){
      return null;
    }
    Tuple t = new DefaultTuple();
    for (int i = 0; i< hr.size(); i++){
      t.append(extractPigObject(hr.get(i),org.apache.hadoop.hive.howl.data.DataType.findType(hr.get(i))));
    }
    return t;
  }

  public static Object extractPigObject(Object o) throws Exception{
    return extractPigObject(o,org.apache.hadoop.hive.howl.data.DataType.findType(o));
  }

  @SuppressWarnings("unchecked")
  public static Object extractPigObject(Object o, byte itemType) throws Exception{
    if (
        (itemType != org.apache.hadoop.hive.howl.data.DataType.LIST)
        && (itemType != org.apache.hadoop.hive.howl.data.DataType.STRUCT)
        && (itemType != org.apache.hadoop.hive.howl.data.DataType.MAP)){
      // primitive type.
      if (itemType != org.apache.hadoop.hive.howl.data.DataType.SHORT){
        // return new Integer(((Short)o).intValue());
        return o; // we seem to be getting a runtime Integer here anyway.
      } else if (itemType != org.apache.hadoop.hive.howl.data.DataType.BYTE){
        // return new Integer(((Byte)o).intValue());
        return new Integer(((Short)o).intValue()); // we're getting a runtime Short here.
      }else{
        return o;
      }
    } else  if (itemType == org.apache.hadoop.hive.howl.data.DataType.STRUCT) {
      return transformToTuple((HowlRecord) o);
    } else  if (itemType == org.apache.hadoop.hive.howl.data.DataType.LIST) {
      return transformToBag((List<? extends Object>) o);
    } else  if (itemType == org.apache.hadoop.hive.howl.data.DataType.MAP) {
      return transformToPigMap((Map<String, Object>)o);
    }
    return null; // never invoked.
  }

  public static Map<String,Object> transformToPigMap(Map<String,Object> map) throws Exception {
    Map<String,Object> txmap = new HashMap<String,Object>();
    for (Entry<String,Object> e : map.entrySet()){
      txmap.put(e.getKey(), extractPigObject(e.getValue()));
    }
    return txmap;
  }

  public static DataBag transformToBag(List<? extends Object> list) throws Exception {
    DataBag db = new DefaultDataBag();
    if (list.isEmpty()){
      return db;
    }
    byte itemType = org.apache.hadoop.hive.howl.data.DataType.findType(list.get(0));
    if (itemType != org.apache.hadoop.hive.howl.data.DataType.STRUCT){
      // for items inside a bag that are not directly tuples themselves, we need to wrap inside a tuple
      for (Object o : list){
        Tuple innerTuple = new DefaultTuple();
        innerTuple.append(extractPigObject(o,itemType));
        db.add(innerTuple);
      }
    }else{
      for (Object o : list){
        db.add(transformToTuple((HowlRecord) o));
      }
    }
    return db;
  }


}
