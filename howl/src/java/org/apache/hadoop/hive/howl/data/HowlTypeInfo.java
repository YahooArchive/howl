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
package org.apache.hadoop.hive.howl.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class HowlTypeInfo extends TypeInfo {

  TypeInfo baseTypeInfo = null;

  // populated if the base type is a struct
  List<HowlTypeInfo> listFields = null;

  // populated if base type is a list
  HowlTypeInfo listType = null;

  // populated if the base type is a map
  HowlTypeInfo mapKeyType = null;
  HowlTypeInfo mapValueType = null;

  @SuppressWarnings("unused")
  private HowlTypeInfo(){
    // preventing empty ctor from being callable
  }

  /**
   * Instantiates a HowlTypeInfo from a Schema object.
   * @param schema The base Schema
   */
  public HowlTypeInfo(Schema schema){
    List<FieldSchema> fields = schema.getFieldSchemas();
    List<String> names = new ArrayList<String>();
    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    for (FieldSchema f : fields){
      names.add(f.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(f.getType()));
    }
    this.baseTypeInfo = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
    deepTraverseAndSetup();
  }

  /**
   * Instantiates a HowlTypeInfo from a FieldSchema object.
   * @param fieldSchema The base FieldSchema
   */
  HowlTypeInfo(FieldSchema fieldSchema){
    this(fieldSchema.getType());
  }

  /**
   * Instantiates a HowlTypeInfo from a string representation of the underlying type.
   * @param type The base type representation
   */
  public HowlTypeInfo(String type) {
    this(TypeInfoUtils.getTypeInfoFromTypeString(type));
  }

  /**
   * Instantiating a HowlTypeInfo overlaying a underlying TypeInfo
   * @param typeInfo the base TypeInfo
   */
  public HowlTypeInfo(TypeInfo typeInfo){
    this.baseTypeInfo = typeInfo;
    deepTraverseAndSetup();
  }

  private void deepTraverseAndSetup() {
    if (baseTypeInfo.getCategory() == Category.MAP){
      mapKeyType = new HowlTypeInfo(((MapTypeInfo)baseTypeInfo).getMapKeyTypeInfo());
      mapValueType = new HowlTypeInfo(((MapTypeInfo)baseTypeInfo).getMapValueTypeInfo());
    }else if (baseTypeInfo.getCategory() == Category.LIST){
      listType = new HowlTypeInfo(((ListTypeInfo)baseTypeInfo).getListElementTypeInfo());
    }else if (baseTypeInfo.getCategory() == Category.STRUCT){
      for(TypeInfo ti : ((StructTypeInfo)baseTypeInfo).getAllStructFieldTypeInfos()){
        listFields.add(new HowlTypeInfo(ti));
      }
    }
  }

  // TODO : throw exception if null? do we want null or exception semantics?
  // (Currently going with null semantics)

  /**
   * Get the underlying map key type (if the underlying TypeInfo is a map type)
   */
  public HowlTypeInfo getMapKeyTypeInfo(){
    return mapKeyType;
  }

  /**
   * Get the underlying map value type (if the underlying TypeInfo is a map type)
   */
  public HowlTypeInfo getMapValueTypeInfo(){
    return mapValueType;
  }

  /**
   * Get the underlying list element type (if the underlying TypeInfo is a list type)
   */
  public HowlTypeInfo getListElementTypeInfo(){
    return listType;
  }

  /**
   * Get the underlying struct element types (if the underlying TypeInfo is a struct type)
   */
  public List<HowlTypeInfo> getAllStructFieldTypeInfos(){
    return listFields;
  }

  public TypeInfo getBaseTypeInfo(){
    return baseTypeInfo;
  }

  @Override
  public Category getCategory() {
    return baseTypeInfo.getCategory();
  }

  @Override
  public String getTypeName() {
    return baseTypeInfo.getTypeName();
  }

}
