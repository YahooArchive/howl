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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.data.HowlArrayBag;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.howl.data.Pair;
import org.apache.hadoop.hive.howl.data.type.HowlType;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfo;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfoUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
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
import org.apache.pig.impl.util.UDFContext;

public class PigHowlUtil {

  public static final String HOWL_TABLE_SCHEMA = "howl.table.schema";
  public static final String HOWL_METASTORE_URI = "howl.metastore.uri";

  static final int PIG_EXCEPTION_CODE = 1115; // http://wiki.apache.org/pig/PigErrorHandlingFunctionalSpecification#Error_codes
  private static final String DEFAULT_DB = MetaStoreUtils.DEFAULT_DATABASE_NAME;

  private final  Map<Pair<String,String>, Table> howlTableCache =
    new HashMap<Pair<String,String>, Table>();

  static public Pair<String, String> getDBTableNames(String location) throws IOException {
    // the location string will be of the form:
    // <database name>.<table name> - parse it and
    // communicate the information to HowlInputFormat

    String[] dbTableNametokens = location.split("\\.");
    if(dbTableNametokens.length == 1) {
      return new Pair<String,String>(DEFAULT_DB,location);
    }else if (dbTableNametokens.length == 2) {
      return new Pair<String, String>(dbTableNametokens[0], dbTableNametokens[1]);
    }else{
      String locationErrMsg = "The input location in load statement " +
      "should be of the form " +
      "<databasename>.<table name> or <table name>. Got " + location;
      throw new PigException(locationErrMsg, PIG_EXCEPTION_CODE);
    }
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
      throw new PigException("Table not found : " + nsoe.getMessage(), PIG_EXCEPTION_CODE); // prettier error messages to frontend
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

    List<ResourceFieldSchema> rfSchemaList = new ArrayList<ResourceFieldSchema>();
    for (HowlFieldSchema hfs : howlSchema.getHowlFieldSchemas()){
      ResourceFieldSchema rfSchema;
      rfSchema = getResourceSchemaFromFieldSchema(hfs);
      rfSchemaList.add(rfSchema);
    }
    ResourceSchema rSchema = new ResourceSchema();
    rSchema.setFields(rfSchemaList.toArray(new ResourceFieldSchema[0]));
    return rSchema;

  }

  private ResourceFieldSchema getResourceSchemaFromFieldSchema(HowlFieldSchema hfs)
      throws IOException {
    ResourceFieldSchema rfSchema;
    // if we are dealing with a bag or tuple column - need to worry about subschema
    if(hfs.getHowlTypeInfo().getType() == HowlType.STRUCT) {
        rfSchema = new ResourceFieldSchema()
          .setName(hfs.getName())
          .setDescription(hfs.getComment())
          .setType(getPigType( HowlTypeInfoUtils.getHowlTypeInfo(hfs.getType())))
          .setSchema(getTupleSubSchema(hfs.getHowlTypeInfo()));
    } else if(hfs.getHowlTypeInfo().getType() == HowlType.ARRAY) {
      rfSchema = new ResourceFieldSchema()
      .setName(hfs.getName())
      .setDescription(hfs.getComment())
      .setType(getPigType( HowlTypeInfoUtils.getHowlTypeInfo(hfs.getType())))
      .setSchema(getBagSubSchema(hfs.getHowlTypeInfo()));
    } else {
      rfSchema = new ResourceFieldSchema()
                .setName(hfs.getName())
                .setDescription(hfs.getComment())
                .setType(getPigType( HowlTypeInfoUtils.getHowlTypeInfo(hfs.getType()) ))
                .setSchema(null); // no munging inner-schemas
    }

    return rfSchema;
  }

  private ResourceSchema getBagSubSchema(HowlTypeInfo howlTypeInfo) throws IOException {
    // there are two cases - array<Type> and array<struct<...>>
    // in either case the element type of the array is represented in a
    // tuple field schema in the bag's field schema - the second case (struct)
    // more naturally translates to the tuple - in the first case (array<Type>)
    // we simulate the tuple by putting the single field in a tuple
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("innerTuple")
      .setDescription("The tuple in the bag")
      .setType(DataType.TUPLE);
    HowlTypeInfo arrayElementTypeInfo = howlTypeInfo.getListElementTypeInfo();
    if(arrayElementTypeInfo.getType() == HowlType.STRUCT) {
      bagSubFieldSchemas[0].setSchema(getTupleSubSchema(arrayElementTypeInfo));
    } else {
      ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
      innerTupleFieldSchemas[0] = new ResourceFieldSchema().setName("innerField")
        .setDescription("The inner field in the tuple in the bag")
        .setType(getPigType(arrayElementTypeInfo))
        .setSchema(null); // the element type is not a tuple - so no subschema
      bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    }
    return new ResourceSchema().setFields(bagSubFieldSchemas);

  }

  private ResourceSchema getTupleSubSchema(HowlTypeInfo structTypeInfo) throws IOException {
    // for each struct subfield, create equivalent HowlFieldSchema corresponding
    // to the relevant HowlTypeInfo and then create equivalent ResourceFieldSchema
    ResourceSchema s = new ResourceSchema();
    List<ResourceFieldSchema> lrfs = new ArrayList<ResourceFieldSchema>();
    List<HowlTypeInfo> structFieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
    List<String> structFieldNames = HowlTypeInfoUtils.getStructFieldNames(structTypeInfo);
    for(int i = 0; i < structFieldTypeInfos.size(); i++) {
      HowlFieldSchema hfs = new HowlFieldSchema(structFieldNames.get(i),
          structFieldTypeInfos.get(i).getTypeString(), "");
      lrfs.add(getResourceSchemaFromFieldSchema(hfs));
    }
    s.setFields(lrfs.toArray(new ResourceFieldSchema[0]));
    return s;
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
      throw new PigException(errMsg, PIG_EXCEPTION_CODE);
    }

    errMsg = "Howl column type '"+ type.toString() +"' is not supported in Pig as a column type";
    throw new PigException(errMsg, PIG_EXCEPTION_CODE);
  }


  public static Tuple transformToTuple(HowlRecord hr, HowlTypeInfo hti) throws Exception {
    if (hr == null){
      return null;
    }
    return transformToTuple(hr.getAll(),hti);
  }

  @SuppressWarnings("unchecked")
  public static Object extractPigObject(Object o, HowlTypeInfo hti) throws Exception{
    HowlType itemType = hti.getType();
    if ( ! HowlTypeInfoUtils.isComplex(itemType)){
      return o;
    } else  if (itemType == HowlType.STRUCT) {
      return transformToTuple((List<Object>)o,hti);
    } else  if (itemType == HowlType.ARRAY) {
      return transformToBag((List<? extends Object>) o,hti);
    } else  if (itemType == HowlType.MAP) {
      return transformToPigMap((Map<String, Object>)o,hti);
    }
    return null; // never invoked.
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

  public static Map<String,Object> transformToPigMap(Map<String,Object> map, HowlTypeInfo hti) throws Exception {
    return map;
  }

  @SuppressWarnings("unchecked")
  public static DataBag transformToBag(List<? extends Object> list, HowlTypeInfo hti) throws Exception {
    if (list == null){
      return null;
    }

    HowlTypeInfo elementTypeInfo;
    if ((elementTypeInfo = hti.getListElementTypeInfo()).getType() == HowlType.STRUCT){
      DataBag db = new DefaultDataBag();
      for (Object o : list){
        db.add(transformToTuple((List<Object>)o,elementTypeInfo));
      }
      return db;
    }else {
      return  new HowlArrayBag(list);
    }
  }

  public static void validateHowlTableSchemaFollowsPigRules(HowlSchema howlTableSchema) throws IOException {
    HowlTypeInfo tableTypeInfo = HowlTypeInfoUtils.getHowlTypeInfo(howlTableSchema);
    if (tableTypeInfo.getType() != HowlType.STRUCT){
      throw new PigException("Table level schema was not a struct type schema!");
    }
    for (HowlTypeInfo hti : tableTypeInfo.getAllStructFieldTypeInfos()){
      HowlType htype = hti.getType();
      if (htype == HowlType.ARRAY){
        validateIsPigCompatibleArrayWithPrimitivesOrSimpleComplexTypes(hti);
      }else if (htype == HowlType.STRUCT){
        validateIsPigCompatibleStructWithPrimitives(hti);
      }else if (htype == HowlType.MAP){
        validateIsPigCompatibleMapWithPrimitives(hti);
      }else {
        validateIsPigCompatiblePrimitive(hti);
      }
    }
  }

  private static void validateIsPigCompatibleArrayWithPrimitivesOrSimpleComplexTypes(
      HowlTypeInfo hti) throws IOException {
    HowlTypeInfo subTypeInfo = hti.getListElementTypeInfo();
    if (subTypeInfo.getType() == HowlType.STRUCT){
      validateIsPigCompatibleStructWithPrimitives(subTypeInfo);
    } else if (subTypeInfo.getType() == HowlType.MAP) {
      validateIsPigCompatiblePrimitive(subTypeInfo.getMapValueTypeInfo());
    }
  }

  private static void validateIsPigCompatibleMapWithPrimitives(HowlTypeInfo hti) throws IOException{
    if ( hti.getMapKeyTypeInfo().getType() != HowlType.STRING){
      throw new PigException("Incompatible type in schema, found map with " +
      		"non-string key type in :"+hti.getTypeString(), PIG_EXCEPTION_CODE);
    }
    validateIsPigCompatiblePrimitive(hti.getMapValueTypeInfo());
  }

  private static void validateIsPigCompatibleStructWithPrimitives(HowlTypeInfo hti)
      throws IOException {
    for (HowlTypeInfo subFieldType : hti.getAllStructFieldTypeInfos()){
      validateIsPigCompatiblePrimitive(subFieldType);
    }
  }

  private static void validateIsPigCompatiblePrimitive(HowlTypeInfo hti) throws IOException {
    HowlType htype = hti.getType();
    if (
        (HowlTypeInfoUtils.isComplex(htype)) ||
        (htype == HowlType.TINYINT) ||
        (htype == HowlType.SMALLINT)
        ){
      throw new PigException("Incompatible type in schema, expected pig " +
      		"compatible primitive for:" + hti.getTypeString());
    }
  }

}
