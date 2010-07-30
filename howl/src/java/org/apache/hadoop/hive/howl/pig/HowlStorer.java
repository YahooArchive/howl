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

package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.howl.data.type.HowlType;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfo;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfoUtils;
import org.apache.hadoop.hive.howl.mapreduce.HowlOutputFormat;
import org.apache.hadoop.hive.howl.mapreduce.HowlTableInfo;
import org.apache.hadoop.hive.howl.mapreduce.HowlUtil;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

/**
 * HowlStorer.
 *
 */


public class HowlStorer extends StoreFunc {

  /**
   *
   */
  private static final String COMPUTED_OUTPUT_SCHEMA = "howl.output.schema";
  private final Map<String,String> partitions;
  private Schema pigSchema;
  private static final Log log = LogFactory.getLog(HowlStorer.class);
  private RecordWriter<WritableComparable<?>, HowlRecord> writer;

  public HowlStorer(String partSpecs, String schema) throws ParseException {

    String[] partKVPs = partSpecs.split(",");
    partitions = new HashMap<String, String>(partKVPs.length);
    for(String partKVP : partKVPs){
      String[] partKV = partKVP.split("=");
      if(partKV.length == 2) {
        partitions.put(partKV[0].trim(), partKV[1].trim());
      }
    }

    if(schema != null) {
      pigSchema = Utils.getSchemaFromString(schema);
    }
  }

  public HowlStorer(String partSpecs) throws ParseException {
   this(partSpecs, null);
  }

  @Override
  public void checkSchema(ResourceSchema resourceSchema) throws IOException {

    /*  Schema provided by user and the schema computed by Pig
     * at the time of calling store must match.
     */
    Schema runtimeSchema = Schema.getPigSchema(resourceSchema);
    if(pigSchema != null){
      if(! Schema.equals(runtimeSchema, pigSchema, false, false) ){
        throw new FrontendException("Schema provided in store statement doesn't match with the Schema" +
      "returned by Pig run-time. Schema provided in HowlStorer: "+pigSchema.toString()+ " Schema received from Pig runtime: "+runtimeSchema.toString());
      }
    } else {
      pigSchema = runtimeSchema;
    }
  }

  /** Constructs HowlSchema from pigSchema. Passed tableSchema is the existing
   * schema of the table in metastore.
   */
  private HowlSchema convertPigSchemaToHowlSchema(Schema pigSchema, HowlSchema tableSchema) throws FrontendException{

    List<HowlFieldSchema> fieldSchemas = new ArrayList<HowlFieldSchema>(pigSchema.size());
    for(FieldSchema fSchema : pigSchema.getFields()){
      byte type = fSchema.type;
      HowlFieldSchema howlFSchema;
      String alias = fSchema.alias;
      if(type == org.apache.pig.data.DataType.BAG){

        // Find out if we need to throw away the tuple or not.
        HowlTypeInfo typeInfo = removeTupleFromBag(tableSchema, fSchema) ?
            HowlTypeInfoUtils.getListHowlTypeInfo(getTypeInfoFrom(fSchema.schema.getField(0).schema.getField(0))) : getTypeInfoFrom(fSchema);
        howlFSchema = new HowlFieldSchema(alias,typeInfo.getTypeString(),"");
      }
      else if(type == org.apache.pig.data.DataType.TUPLE ){
        howlFSchema = new HowlFieldSchema(alias,getTypeInfoFrom(fSchema).getTypeString(),"");
      }
      else if( type == DataType.MAP){
        howlFSchema = new HowlFieldSchema(alias,getTypeInfoFrom(fSchema).getTypeString(),"");
      }
      else{
        howlFSchema = new HowlFieldSchema(alias,getHiveTypeString(type),"");
      }
      fieldSchemas.add(howlFSchema);
    }

    return new HowlSchema(fieldSchemas);
  }

  private void validateUnNested(Schema innerSchema) throws FrontendException{

    for(FieldSchema innerField : innerSchema.getFields()){
      if(DataType.isComplex(innerField.type)) {
        throw new FrontendException("Complex types cannot be nested.");
      }
    }
  }

  private boolean removeTupleFromBag(HowlSchema tableSchema, FieldSchema bagFieldSchema){

    String colName = bagFieldSchema.alias;
    for(HowlFieldSchema field : tableSchema.getHowlFieldSchemas()){
      if(colName.equals(field.getName())){
        List<HowlTypeInfo> tupleTypeInfo = field.getHowlTypeInfo().getListElementTypeInfo().getAllStructFieldTypeInfos();
        return (tupleTypeInfo == null || tupleTypeInfo.size() == 0) ? true : false;
      }
    }
    // Column was not found in table schema. Its a new column
    List<FieldSchema> tupSchema = bagFieldSchema.schema.getFields();
    return (tupSchema.size() == 1 && tupSchema.get(0).schema == null) ? true : false;
  }


  private HowlTypeInfo getTypeInfoFrom(FieldSchema fSchema) throws FrontendException{

    byte type = fSchema.type;
    switch(type){

    case DataType.BAG:
      Schema bagSchema = fSchema.schema;
      return HowlTypeInfoUtils.getListHowlTypeInfo(getTypeInfoFrom(bagSchema.getField(0)));

    case DataType.TUPLE:
      List<String> fieldNames = new ArrayList<String>();
      List<HowlTypeInfo> typeInfos = new ArrayList<HowlTypeInfo>();
      for( FieldSchema fieldSchema : fSchema.schema.getFields()){
        fieldNames.add( fieldSchema.alias);
        typeInfos.add(getTypeInfoFrom(fieldSchema));
      }
      return HowlTypeInfoUtils.getStructHowlTypeInfo(fieldNames, typeInfos);

    case DataType.MAP:
      // Pig's schema contain no type information about map's keys and
      // values. So, if its a new column assume <string,string> if its existing
      // return whatever is contained in the existing column.
      HowlFieldSchema mapField = getTableCol(fSchema.alias, howlTblSchema);
      if(mapField != null){
        HowlType mapValType = mapField.getHowlTypeInfo().getMapValueTypeInfo().getType();
        Class<?> clazz;
        switch(mapValType){
        case STRING:
          clazz = String.class;
          break;
        case BIGINT:
          clazz = Long.class;
          break;
        case INT:
          clazz = Integer.class;
          break;
        case FLOAT:
          clazz = Float.class;
          break;
        case DOUBLE:
          clazz = Double.class;
          break;
          default:
            throw new FrontendException("Only pig primitive types are supported as map value types.");
        }
        return HowlTypeInfoUtils.getMapHowlTypeInfo(HowlTypeInfoUtils.getPrimitiveTypeInfo(String.class), HowlTypeInfoUtils.getPrimitiveTypeInfo(clazz));
      }

      return HowlTypeInfoUtils.getMapHowlTypeInfo(HowlTypeInfoUtils.getPrimitiveTypeInfo(String.class), HowlTypeInfoUtils.getPrimitiveTypeInfo(String.class));
    default:
      return HowlTypeInfoUtils.getHowlTypeInfo(getHiveTypeString(type));
    }

  }

  private String getHiveTypeString(byte type) throws FrontendException{

    switch(type){

    case DataType.CHARARRAY:
    case DataType.BIGCHARARRAY:
      return "string";
    case DataType.LONG:
      return "bigint";
    case DataType.BYTEARRAY:
      throw new FrontendException("HowlStorer expects typed data. Cannot write bytearray.");
    default:
      return DataType.findTypeName(type);
    }
  }


  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new HowlOutputFormat();
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
    computedSchema = (HowlSchema)ObjectSerializer.deserialize(UDFContext.getUDFContext().getUDFProperties(this.getClass()).getProperty(COMPUTED_OUTPUT_SCHEMA));
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {

    List<Object> outgoing = new ArrayList<Object>(tuple.size());

    int i = 0;
    for(HowlFieldSchema fSchema : computedSchema.getHowlFieldSchemas()){
      outgoing.add(getJavaObj(tuple.get(i++), fSchema.getHowlTypeInfo()));
    }

    try {
      writer.write(null, new DefaultHowlRecord(outgoing));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private Object getJavaObj(Object pigObj, HowlTypeInfo typeInfo) throws ExecException{

    // The real work-horse. Spend time and energy in this method if there is
    // need to keep HowlStorer lean and go fast.
    HowlType type = typeInfo.getType();

    switch(type){
      case STRUCT:
        // Unwrap the tuple.
      Tuple innerTup = (Tuple)pigObj;
      List<Object> innerList = new ArrayList<Object>(innerTup.size());
      int i = 0;
      for(HowlTypeInfo structFieldTypeInfo : typeInfo.getAllStructFieldTypeInfos()){
        innerList.add(getJavaObj(innerTup.get(i++), structFieldTypeInfo));
      }
      return innerList;
    case ARRAY:
      // Unwrap the bag.
      DataBag pigBag = (DataBag)pigObj;
      HowlTypeInfo tupTypeInfo = typeInfo.getListElementTypeInfo();
      List<Object> bagContents = new ArrayList<Object>((int)pigBag.size());
      Iterator<Tuple> bagItr = pigBag.iterator();

      while(bagItr.hasNext()){
        if(tupTypeInfo.getAllStructFieldTypeInfos() == null){
          // If there is only one element in tuple contained in bag, we throw away the tuple.
          bagContents.add(getJavaObj(bagItr.next().get(0),tupTypeInfo));
        } else {
          bagContents.add(getJavaObj(bagItr.next(), tupTypeInfo));
        }
      }
      return bagContents;

//    case MAP:
//     Map<String,DataByteArray> pigMap = (Map<String,DataByteArray>)pigObj;
//     Map<String,Long> typeMap = new HashMap<String, Long>();
//     for(Entry<String, DataByteArray> entry: pigMap.entrySet()){
//       typeMap.put(entry.getKey(), new Long(entry.getValue().toString()));
//     }
//     return typeMap;
    default:
      return pigObj;
    }
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {

    // Need to necessarily override this method since default impl assumes HDFS
    // based location string.
    return location;
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
  }

  HowlSchema computedSchema;

  private void doSchemaValidations(Schema pigSchema, HowlSchema tblSchema) throws FrontendException{

    // Iterate through all the elements in Pig Schema and do validations as
    // dictated by semantics, consult HowlSchema of table when need be.

    for(FieldSchema pigField : pigSchema.getFields()){
      byte type = pigField.type;
      String alias = pigField.alias;
      HowlFieldSchema howlField = getTableCol(alias, tblSchema);
      if(DataType.isComplex(type)){
        switch(type){

        case DataType.MAP:
          if(howlField != null){
            if(howlField.getHowlTypeInfo().getMapKeyTypeInfo().getType() != HowlType.STRING){
              throw new FrontendException("Key Type of map must be String");
            }
            if(HowlTypeInfoUtils.isComplex(howlField.getHowlTypeInfo().getMapValueTypeInfo().getType())){
              throw new FrontendException("Value type of map cannot be complex");
            }
          }
          break;

        case DataType.BAG:
          // Only map is allowed as complex type in tuples inside bag.
          for(FieldSchema innerField : pigField.schema.getField(0).schema.getFields()){
            if(innerField.type == DataType.BAG || innerField.type == DataType.TUPLE) {
              throw new FrontendException("Complex types cannot be nested. "+innerField);
            }
          }
          if(howlField != null){
            // Do the same validation for HowlSchema.
            HowlTypeInfo listTypeInfo = howlField.getHowlTypeInfo().getListElementTypeInfo();
            HowlType hType = listTypeInfo.getType();
            if(hType == HowlType.STRUCT){
              for(HowlTypeInfo structTypeInfo : listTypeInfo.getAllStructFieldTypeInfos()){
                if(structTypeInfo.getType() == HowlType.STRUCT || structTypeInfo.getType() == HowlType.ARRAY){
                  throw new FrontendException("Nested Complex types not allowed");
                }
              }
            }
            if(hType == HowlType.MAP){
              if(listTypeInfo.getMapKeyTypeInfo().getType() != HowlType.STRING){
                throw new FrontendException("Key Type of map must be String");
              }
              if(HowlTypeInfoUtils.isComplex(listTypeInfo.getMapValueTypeInfo().getType())){
                throw new FrontendException("Value type of map cannot be complex");
              }
            }
             if(hType == HowlType.ARRAY) {
              throw new FrontendException("Arrays cannot contain array within it.");
            }
          }
          break;

        case DataType.TUPLE:
          validateUnNested(pigField.schema);
          if(howlField != null){
            for(HowlTypeInfo typeInfo : howlField.getHowlTypeInfo().getAllStructFieldTypeInfos()){
              if(HowlTypeInfoUtils.isComplex(typeInfo.getType())){
                throw new FrontendException("Nested Complex types are not allowed.");
              }
            }
          }
          break;

        default:
          throw new FrontendException("Internal Error.");
        }
      }
    }

    for(HowlFieldSchema howlField : tblSchema.getHowlFieldSchemas()){

      // We dont do type promotion/demotion.
      HowlType hType = howlField.getHowlTypeInfo().getType();
      switch(hType){
      case SMALLINT:
      case TINYINT:
      case BOOLEAN:
        throw new FrontendException("Incompatible found in howl table schema: "+hType);
      }
    }
  }


  // Finds column by name in HowlSchema, if not found returns null.
  private HowlFieldSchema getTableCol(String alias, HowlSchema tblSchema){

    for(HowlFieldSchema howlField : tblSchema.getHowlFieldSchemas()){
      if(howlField.getName().equals(alias)){
        return howlField;
      }
    }
    // Its a new column
    return null;
  }
  HowlSchema howlTblSchema;

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {

    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());

    String[] userStr = location.split("\\.");
    if(userStr.length != 2) {
      throw new IOException("Incorrect store location. Please, specify the store location as dbname.tblname");
    }
    HowlTableInfo tblInfo = HowlTableInfo.getOutputTableInfo(PigHowlUtil.getHowlServerUri(),
        userStr[0],userStr[1],partitions);


    Configuration config = job.getConfiguration();
    if(!HowlUtil.checkJobContextIfRunningFromBackend(job)){

      HowlOutputFormat.setOutput(job, tblInfo);
      howlTblSchema = HowlOutputFormat.getTableSchema(job);
      doSchemaValidations(pigSchema, howlTblSchema);
      computedSchema = convertPigSchemaToHowlSchema(pigSchema,howlTblSchema);
      HowlOutputFormat.setSchema(job, computedSchema);
      p.setProperty(HowlOutputFormat.HOWL_KEY_OUTPUT_INFO, config.get(HowlOutputFormat.HOWL_KEY_OUTPUT_INFO));
      if(config.get(HowlOutputFormat.HOWL_KEY_HIVE_CONF) != null){
        p.setProperty(HowlOutputFormat.HOWL_KEY_HIVE_CONF, config.get(HowlOutputFormat.HOWL_KEY_HIVE_CONF));
      }
      p.setProperty(COMPUTED_OUTPUT_SCHEMA,ObjectSerializer.serialize(computedSchema));

    }else{
      config.set(HowlOutputFormat.HOWL_KEY_OUTPUT_INFO, p.getProperty(HowlOutputFormat.HOWL_KEY_OUTPUT_INFO));
      if(p.getProperty(HowlOutputFormat.HOWL_KEY_HIVE_CONF) != null){
        config.set(HowlOutputFormat.HOWL_KEY_HIVE_CONF, p.getProperty(HowlOutputFormat.HOWL_KEY_HIVE_CONF));
      }
    }
  }
}
