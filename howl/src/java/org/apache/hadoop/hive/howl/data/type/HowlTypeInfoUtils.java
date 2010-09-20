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
package org.apache.hadoop.hive.howl.data.type;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.DeprecatedHowlSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.mapreduce.HowlUtil;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public class HowlTypeInfoUtils {

  private static HowlTypeInfoUtils ref = new HowlTypeInfoUtils();
  public static MapBuilder getMapHowlTypeInfoBuilder(){
    return ref.new MapBuilder();
  }

  public static StructBuilder getStructHowlTypeInfoBuilder(){
    return ref.new StructBuilder();
  }

  public static HowlTypeInfo getMapHowlTypeInfo(HowlTypeInfo keyType, HowlTypeInfo valueType){
    return getMapHowlTypeInfoBuilder().withKeyType(keyType).withValueType(valueType).build();
  }

  public static HowlTypeInfo getStructHowlTypeInfo(List<String> names, List<HowlTypeInfo> fieldTypes){
    StructBuilder sbuilder = getStructHowlTypeInfoBuilder();
    for (int i = 0; i< names.size(); i++){
      sbuilder.addField(names.get(i), fieldTypes.get(i));
    }
    return sbuilder.build();
  }

  public static HowlTypeInfo getListHowlTypeInfo(HowlTypeInfo listItemType){
    return new HowlTypeInfo(
        TypeInfoFactory.getListTypeInfo(listItemType.getTypeInfo())
        );
  }

  public static HowlTypeInfo getPrimitiveHowlTypeInfo(Object o){
    return getPrimitiveHowlTypeInfo(o.getClass());
  }

  public static HowlTypeInfo getPrimitiveTypeInfo(Class clazz){
    return new HowlTypeInfo(
        TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(clazz)
        );
  }

  public static HowlTypeInfo getHowlTypeInfo(Schema s){
    return getHowlTypeInfo(s.getFieldSchemas());
  }

  public static HowlTypeInfo getHowlTypeInfo(DeprecatedHowlSchema s){
    return getHowlTypeInfo(s.getHowlFieldSchemas());
  }
  
  public static HowlTypeInfo getHowlTypeInfo(HowlSchema s){
      return getHowlTypeInfo(HowlUtil.getFieldSchemaList(s.getFields()));
  }

  public static HowlTypeInfo getHowlTypeInfo(List<? extends FieldSchema> fslist){
    StructBuilder sbuilder = getStructHowlTypeInfoBuilder();
    for (FieldSchema fieldSchema : fslist){
      sbuilder.addField(fieldSchema.getName(),fieldSchema.getType());
    }
    return sbuilder.build();
  }

  public static HowlTypeInfo getHowlTypeInfo(String typeString){
    return new HowlTypeInfo(typeString);
  }

  public static boolean isComplex(HowlFieldSchema.Type type){
    return type == HowlFieldSchema.Type.ARRAY || type == HowlFieldSchema.Type.MAP || type == HowlFieldSchema.Type.STRUCT;
  }

  public static List<String> getStructFieldNames(HowlTypeInfo structTypeInfo) {
    if(structTypeInfo.type != HowlFieldSchema.Type.STRUCT) {
      throw new IllegalArgumentException("Expected struct, got non struct type " +
          "info");
    }
    
    return ((StructTypeInfo)structTypeInfo.getTypeInfo()).getAllStructFieldNames();
    
  }
  
  public abstract class HowlTypeInfoBuilder {
    public abstract HowlTypeInfo build();
  }

  public class MapBuilder extends HowlTypeInfoBuilder {

    HowlTypeInfo keyTypeInfo = null;
    HowlTypeInfo valueTypeInfo = null;

    @Override
    public HowlTypeInfo build() {
      return new HowlTypeInfo(
          TypeInfoFactory.getMapTypeInfo(keyTypeInfo.getTypeInfo(), valueTypeInfo.getTypeInfo())
      );
    }

    public MapBuilder withValueType(HowlTypeInfo valueType) {
      this.valueTypeInfo = valueType;
      return this;
    }

    public MapBuilder withValueType(TypeInfo valueType) {
      return this.withValueType(new HowlTypeInfo(valueType));
    }

    public MapBuilder withKeyType(HowlTypeInfo keyType) {
      this.keyTypeInfo = keyType;
      return this;
    }

    public MapBuilder withKeyType(TypeInfo keyType) {
      return this.withKeyType(new HowlTypeInfo(keyType));
    }

  }

  public class StructBuilder extends HowlTypeInfoBuilder {
    List<String> fieldNames = null;
    List<TypeInfo> fieldTypeInfos = null;

    StructBuilder(){
      fieldNames = new ArrayList<String>();
      fieldTypeInfos = new ArrayList<TypeInfo>();
    }

    public StructBuilder addField(String fieldName, String fieldType) {
      return this.addField(fieldName, TypeInfoUtils.getTypeInfoFromTypeString(fieldType));
    }

    public StructBuilder addField(String fieldName, HowlTypeInfo fieldType){
      return this.addField(fieldName,fieldType.getTypeInfo());
    }

    public StructBuilder addField(String fieldName, TypeInfo fieldType){
      fieldNames.add(fieldName);
      fieldTypeInfos.add(fieldType);
      return this;
    }

    @Override
    public HowlTypeInfo build() {
      return new HowlTypeInfo(TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos));
    }
    
  }
}

