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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
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
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;

public class PigHowlUtil {

  public static final String HOWL_TABLE_SCHEMA = "howl.table.schema";
  public static final String HOWL_METASTORE_URI = "howl.metastore.uri";

  public static final boolean INJECT_TABLE_COLUMNS_INTO_SCHEMA = true;

  static final int HowlExceptionCode = 4010; // FIXME : edit http://wiki.apache.org/pig/PigErrorHandlingFunctionalSpecification#Error_codes to introduce

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
    Properties props = UDFContext.getUDFContext().getClientSystemProps();
    howlServerUri = props.getProperty(HOWL_METASTORE_URI);
    //    if(howlServerUri == null) {
    //      String msg = "Please provide uri to the metadata server using" +
    //      " -Dhowl.metastore.uri system property";
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
    try {
      client = new HiveMetaStoreClient(hiveConf,null);
    } catch (Exception e){
      throw new Exception("Could not instantiate a HiveMetaStoreClient connecting to server uri:["+serverUri+"]");
    }
    return client;
  }


  HowlSchema getHowlSchema(List<RequiredField> fields, String signature, Class<?> classForUDFCLookup) throws IOException {
    if(fields == null) {
      return null;
    }

    Properties props = UDFContext.getUDFContext().getUDFProperties(
        classForUDFCLookup, new String[] {signature});
    HowlSchema howlTableSchema = (HowlSchema) props.get(HOWL_TABLE_SCHEMA);

    ArrayList<HowlFieldSchema> fcols = new ArrayList<HowlFieldSchema>();
    for(RequiredField rf: fields) {
      fcols.add(howlTableSchema.getHowlFieldSchemas().get(rf.getIndex()));
    }
    return new HowlSchema(fcols);
  }

  public Table getTable(String location) throws IOException {
    return getTable(location,getHowlServerUri());
  }

  public Table getTable(String location, String howlServerUri) throws IOException{
    Pair<String, String> loc_server = new Pair<String,String>(location, howlServerUri);
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
    } catch (NoSuchObjectException nsoe){
      throw new FrontendException("Table not found :" + nsoe.getMessage()); // prettier error messages to frontend
    } catch (Exception e) {
      throw new IOException(e);
    }
    howlTableCache.put(loc_server, table);
    return table;
  }

  public ResourceSchema getResourceSchema(HowlSchema howlSchema, String location) throws IOException {
    if(howlSchema == null) {
      return null;
    }

    Map<String,FieldSchema> mapOfPartitionKeyFieldSchemasByName = new HashMap<String,FieldSchema>();
    if (INJECT_TABLE_COLUMNS_INTO_SCHEMA){
      Table tbl = getTable(location);
      List<FieldSchema> tablePartitionKeysFieldSchemas = tbl.getPartitionKeys();
      for (FieldSchema ptnKeyFieldSchema : tablePartitionKeysFieldSchemas){
        mapOfPartitionKeyFieldSchemasByName.put(ptnKeyFieldSchema.getName(),ptnKeyFieldSchema);
      }
    }

    List<ResourceFieldSchema> rfSchemaList = new ArrayList<ResourceFieldSchema>();
    for (HowlFieldSchema hfs : howlSchema.getHowlFieldSchemas()){
      ResourceFieldSchema rfSchema;
      rfSchema = getResourceSchemaFromFieldSchema(hfs);
      rfSchemaList.add(rfSchema);
      // if one of the specified schema columns has the same name as a ptn key,
      // remove it from the list of columns to be added after this
      if (INJECT_TABLE_COLUMNS_INTO_SCHEMA && (mapOfPartitionKeyFieldSchemasByName.containsKey(hfs.getName()))){
        mapOfPartitionKeyFieldSchemasByName.remove(hfs.getName());
      }
    }
    // now we add in any partition column names that weren't added previously
    if (INJECT_TABLE_COLUMNS_INTO_SCHEMA && (!mapOfPartitionKeyFieldSchemasByName.isEmpty())){
      for (FieldSchema fs : mapOfPartitionKeyFieldSchemasByName.values()){
        ResourceFieldSchema rfSchema;
        rfSchema = getResourceSchemaFromFieldSchema(fs);
        rfSchemaList.add(rfSchema);
      }
    }
    ResourceSchema rSchema = new ResourceSchema();
    rSchema.setFields(rfSchemaList.toArray(new ResourceFieldSchema[0]));
    return rSchema;

  }

  private ResourceFieldSchema getResourceSchemaFromFieldSchema(FieldSchema fs)
      throws IOException {
    ResourceFieldSchema rfSchema;
    rfSchema = new ResourceFieldSchema()
                .setName(fs.getName())
                .setDescription(fs.getComment())
                .setType(getPigType( HowlTypeInfoUtils.getHowlTypeInfo(fs.getType()) ))
                .setSchema(null); // no munging inner-schemas
    return rfSchema;
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


  public static Tuple transformToTuple(HowlRecord hr, HowlTypeInfo hti) throws Exception {
    if (hr == null){
      return null;
    }
    return transformToTuple(hr.getAll(),hti);
  }

  public static Tuple transformToTuple(List<? extends Object> objList, HowlTypeInfo hti) throws Exception {
    if (objList == null){
      return null;
    }
    Tuple t = new DefaultTuple();
    List<HowlTypeInfo> subtypes;
    try {
      subtypes = hti.getAllStructFieldTypeInfos();
    } catch (Exception e){
      if (hti.getType() != HowlType.STRUCT){
        throw new Exception("Expected Struct type, got "+hti.getType());
      } else {
        throw e;
      }
    }
    for (int i = 0; i < subtypes.size(); i++){
      t.append(
            extractPigObject(objList.get(i), subtypes.get(i))
          );
    }
    return t;
  }

  @SuppressWarnings("unchecked")
  public static Object extractPigObject(Object o, HowlTypeInfo hti) throws Exception{
    HowlType itemType = hti.getType();
    if (
        (itemType != HowlType.ARRAY)
        && (itemType != HowlType.STRUCT)
        && (itemType != HowlType.MAP)){
      // primitive type.

      if (itemType == HowlType.SMALLINT){
        return new Integer(((Short)o).intValue());
      } else if (itemType == HowlType.TINYINT){
        return new Integer(((Byte)o).intValue());
      }else{
        return o;
      }
    } else  if (itemType == HowlType.STRUCT) {
      return transformToTuple((List<Object>)o,hti);
    } else  if (itemType == HowlType.ARRAY) {
      return transformToBag((List<? extends Object>) o,hti);
    } else  if (itemType == HowlType.MAP) {
      return transformToPigMap((Map<String, Object>)o,hti);
    }
    return null; // never invoked.
  }

  public static Map<String,Object> transformToPigMap(Map<String,Object> map, HowlTypeInfo hti) throws Exception {
    if (map == null){
      return null;
    }
    Map<String,Object> txmap = new HashMap<String,Object>();
    HowlTypeInfo mapValueTypeInfo;
    try {
      mapValueTypeInfo = hti.getMapValueTypeInfo();
    } catch (Exception e){
      if (hti.getType() != HowlType.MAP){
        throw new Exception("Expected Map type, got "+hti.getType());
      }else{
        throw e;
      }
    }
    for (Entry<String,Object> e : map.entrySet()){
      txmap.put(e.getKey(), extractPigObject(e.getValue(),mapValueTypeInfo));
    }
    return txmap;
  }
  public static DataBag transformToBag(List<? extends Object> list, HowlTypeInfo hti) throws Exception {
    if (list == null){
      return null;
    }

    DataBag db = new DefaultDataBag();
    if (list.isEmpty()){
      return db;
    }
    HowlTypeInfo elementTypeInfo;
    try {
      elementTypeInfo = hti.getListElementTypeInfo();
    } catch (Exception e){
      if (hti.getType() != HowlType.ARRAY){
        throw new Exception("Expected Array type, got "+hti.getType());
      }else{
        throw e;
      }
    }
    if (elementTypeInfo.getType() != HowlType.STRUCT) {
      // for items inside a bag that are not directly tuples themselves, we need to wrap inside a tuple
      for (Object o : list){
        Tuple innerTuple = new DefaultTuple();
        innerTuple.append(extractPigObject(o,elementTypeInfo));
        db.add(innerTuple);
      }
    }else{
      for (Object o : list){
        // each object is a STRUCT type - so we expect that to be a List<Object>
        db.add(transformToTuple((List<Object>)o,elementTypeInfo)); // FIXME : verify that it is indeed List<Object> and not a HowlRecord
      }
    }
    return db;
  }


}
