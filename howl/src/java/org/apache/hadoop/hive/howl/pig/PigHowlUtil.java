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
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.Pair;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema.Type;
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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

public class PigHowlUtil {

  public static final String HOWL_TABLE_SCHEMA = "howl.table.schema";
  public static final String HOWL_METASTORE_URI = "howl.metastore.uri";

  static final int PIG_EXCEPTION_CODE = 1115; // http://wiki.apache.org/pig/PigErrorHandlingFunctionalSpecification#Error_codes
  private static final String DEFAULT_DB = MetaStoreUtils.DEFAULT_DATABASE_NAME;

  private final  Map<Pair<String,String>, Table> howlTableCache =
    new HashMap<Pair<String,String>, Table>();

  private static final TupleFactory tupFac = TupleFactory.getInstance();

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
      fcols.add(howlTableSchema.getFields().get(rf.getIndex()));
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
    for (HowlFieldSchema hfs : howlSchema.getFields()){
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
    if(hfs.getType() == Type.STRUCT) {
        rfSchema = new ResourceFieldSchema()
          .setName(hfs.getName())
          .setDescription(hfs.getComment())
          .setType(getPigType( hfs))
          .setSchema(getTupleSubSchema(hfs));
    } else if(hfs.getType() == Type.ARRAY) {
        rfSchema = new ResourceFieldSchema()
          .setName(hfs.getName())
          .setDescription(hfs.getComment())
          .setType(getPigType( hfs))
          .setSchema(getBagSubSchema(hfs));
    } else {
      rfSchema = new ResourceFieldSchema()
          .setName(hfs.getName())
          .setDescription(hfs.getComment())
          .setType(getPigType( hfs))
          .setSchema(null); // no munging inner-schemas
    }
    return rfSchema;
  }

  private ResourceSchema getBagSubSchema(HowlFieldSchema hfs) throws IOException {
    // there are two cases - array<Type> and array<struct<...>>
    // in either case the element type of the array is represented in a
    // tuple field schema in the bag's field schema - the second case (struct)
    // more naturally translates to the tuple - in the first case (array<Type>)
    // we simulate the tuple by putting the single field in a tuple
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("innertuple")
      .setDescription("The tuple in the bag")
      .setType(DataType.TUPLE);
    HowlFieldSchema arrayElementFieldSchema = hfs.getArrayElementSchema().get(0);
    if(arrayElementFieldSchema.getType() == Type.STRUCT) {
      bagSubFieldSchemas[0].setSchema(getTupleSubSchema(arrayElementFieldSchema));
    } else {
      ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
      innerTupleFieldSchemas[0] = new ResourceFieldSchema().setName("innerfield")
        .setDescription("The inner field in the tuple in the bag")
        .setType(getPigType(arrayElementFieldSchema))
        .setSchema(null); // the element type is not a tuple - so no subschema
      bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    }
    return new ResourceSchema().setFields(bagSubFieldSchemas);

  }

  private ResourceSchema getTupleSubSchema(HowlFieldSchema hfs) throws IOException {
    // for each struct subfield, create equivalent ResourceFieldSchema
    ResourceSchema s = new ResourceSchema();
    List<ResourceFieldSchema> lrfs = new ArrayList<ResourceFieldSchema>();
    for(HowlFieldSchema subField : hfs.getStructSubSchema().getFields()) {
      lrfs.add(getResourceSchemaFromFieldSchema(subField));
    }
    s.setFields(lrfs.toArray(new ResourceFieldSchema[0]));
    return s;
  }

/**
   * @param type owl column type
   * @return corresponding pig type
   * @throws IOException
   */
  static public byte getPigType(HowlFieldSchema hfs) throws IOException {
    return getPigType(hfs.getType());
  }

  static public byte getPigType(Type type) throws IOException {
    String errMsg;

    if (type == Type.STRING){
      return DataType.CHARARRAY;
    }

    if ( (type == Type.INT) || (type == Type.SMALLINT) || (type == Type.TINYINT)){
      return DataType.INTEGER;
    }

    if (type == Type.ARRAY){
      return DataType.BAG;
    }

    if (type == Type.STRUCT){
      return DataType.TUPLE;
    }

    if (type == Type.MAP){
      return DataType.MAP;
    }

    if (type == Type.BIGINT){
      return DataType.LONG;
    }

    if (type == Type.FLOAT){
      return DataType.FLOAT;
    }

    if (type == Type.DOUBLE){
      return DataType.DOUBLE;
    }

    if (type == Type.BOOLEAN){
      errMsg = "Howl column type 'BOOLEAN' is not supported in " +
      "Pig as a column type";
      throw new PigException(errMsg, PIG_EXCEPTION_CODE);
    }

    errMsg = "Howl column type '"+ type.toString() +"' is not supported in Pig as a column type";
    throw new PigException(errMsg, PIG_EXCEPTION_CODE);
  }

  public static Tuple transformToTuple(HowlRecord hr, HowlSchema hs) throws Exception {
      if (hr == null){
        return null;
      }
      return transformToTuple(hr.getAll(),hs);
    }

  @SuppressWarnings("unchecked")
public static Object extractPigObject(Object o, HowlFieldSchema hfs) throws Exception {
      Type itemType = hfs.getType();
      if ( ! hfs.isComplex()){
        return o;
      } else  if (itemType == Type.STRUCT) {
        return transformToTuple((List<Object>)o,hfs);
      } else  if (itemType == Type.ARRAY) {
        return transformToBag((List<? extends Object>) o,hfs);
      } else  if (itemType == Type.MAP) {
        return transformToPigMap((Map<String, Object>)o,hfs);
      }
      return null; // never invoked.
  }

  public static Tuple transformToTuple(List<? extends Object> objList, HowlFieldSchema hfs) throws Exception {
      try {
          return transformToTuple(objList,hfs.getStructSubSchema());
      } catch (Exception e){
          if (hfs.getType() != Type.STRUCT){
              throw new Exception("Expected Struct type, got "+hfs.getType());
          } else {
              throw e;
          }
      }
  }

  public static Tuple transformToTuple(List<? extends Object> objList, HowlSchema hs) throws Exception {
      if (objList == null){
          return null;
        }
        Tuple t = tupFac.newTuple(objList.size());
        List<HowlFieldSchema> subFields = hs.getFields();
        for (int i = 0; i < subFields.size(); i++){
          t.set(i,extractPigObject(objList.get(i), subFields.get(i)));
        }
        return t;
  }

  public static Map<String,Object> transformToPigMap(Map<String,Object> map, HowlFieldSchema hfs) throws Exception {
      return map;
    }

  @SuppressWarnings("unchecked")
  public static DataBag transformToBag(List<? extends Object> list, HowlFieldSchema hfs) throws Exception {
    if (list == null){
      return null;
    }

    HowlFieldSchema elementSubFieldSchema = hfs.getArrayElementSchema().getFields().get(0);
    if (elementSubFieldSchema.getType() == Type.STRUCT){
      DataBag db = new DefaultDataBag();
      for (Object o : list){
        db.add(transformToTuple((List<Object>)o,elementSubFieldSchema));
      }
      return db;
    } else {
      return  new HowlArrayBag(list);
    }
  }


  public static void validateHowlTableSchemaFollowsPigRules(HowlSchema howlTableSchema) throws IOException {
      for (HowlFieldSchema hfs : howlTableSchema.getFields()){
          Type htype = hfs.getType();
          if (htype == Type.ARRAY){
              validateIsPigCompatibleArrayWithPrimitivesOrSimpleComplexTypes(hfs);
          }else if (htype == Type.STRUCT){
              validateIsPigCompatibleStructWithPrimitives(hfs);
          }else if (htype == Type.MAP){
              validateIsPigCompatibleMapWithPrimitives(hfs);
          }else {
              validateIsPigCompatiblePrimitive(hfs);
          }
      }
  }

  private static void validateIsPigCompatibleArrayWithPrimitivesOrSimpleComplexTypes(
          HowlFieldSchema hfs) throws IOException {
      HowlFieldSchema subFieldSchema = hfs.getArrayElementSchema().getFields().get(0);
      if (subFieldSchema.getType() == Type.STRUCT){
          validateIsPigCompatibleStructWithPrimitives(subFieldSchema);
      }else if (subFieldSchema.getType() == Type.MAP) {
          validateIsPigCompatiblePrimitive(subFieldSchema.getMapValueSchema().getFields().get(0));
      }else {
          validateIsPigCompatiblePrimitive(subFieldSchema);
      }
  }

  private static void validateIsPigCompatibleMapWithPrimitives(HowlFieldSchema hfs) throws IOException{
      if (hfs.getMapKeyType() != Type.STRING){
          throw new PigException("Incompatible type in schema, found map with " +
                  "non-string key type in :"+hfs.getTypeString(), PIG_EXCEPTION_CODE);
      }
      validateIsPigCompatiblePrimitive(hfs.getMapValueSchema().getFields().get(0));
  }

  private static void validateIsPigCompatibleStructWithPrimitives(HowlFieldSchema hfs) throws IOException {
      for ( HowlFieldSchema subField : hfs.getStructSubSchema().getFields()){
          validateIsPigCompatiblePrimitive(subField);
      }
  }

  private static void validateIsPigCompatiblePrimitive(HowlFieldSchema hfs) throws IOException {
      Type htype = hfs.getType();
      if (
              (hfs.isComplex()) ||
              (htype == Type.TINYINT) ||
              (htype == Type.SMALLINT)
              ){
            throw new PigException("Incompatible type in schema, expected pig " +
                      "compatible primitive for:" + hfs.getTypeString());
          }

  }

}
