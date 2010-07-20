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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class HowlTypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  HowlType type;

  private TypeInfo baseTypeInfo = null;

  // populated if the base type is a struct
  List<HowlTypeInfo> structFields = null;

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
   * Instantiates a HowlTypeInfo from a string representation of the underlying type.
   * @param type The base type representation
   */
  HowlTypeInfo(String type) {
    this(TypeInfoUtils.getTypeInfoFromTypeString(type));
  }

  /**
   * Instantiating a HowlTypeInfo overlaying a underlying TypeInfo
   * @param typeInfo the base TypeInfo
   */
  HowlTypeInfo(TypeInfo typeInfo){
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
        structFields.add(new HowlTypeInfo(ti));
      }
    } else if(baseTypeInfo.getCategory() == Category.PRIMITIVE) {
        switch(((PrimitiveTypeInfo)baseTypeInfo).getPrimitiveCategory()) {
        case BOOLEAN:
            type = HowlType.BOOLEAN;
            break;
        case BYTE:
            type = HowlType.TINYINT;
            break;
        case DOUBLE:
            type = HowlType.DOUBLE;
            break;
        case FLOAT:
            type = HowlType.FLOAT;
            break;
        case INT:
            type = HowlType.INT;
            break;
        case LONG:
            type = HowlType.BIGINT;
            break;
        case SHORT:
            type = HowlType.SMALLINT;
            break;
        case STRING:
            type = HowlType.STRING;
            break;
        default:
            throw new
            TypeNotPresentException(((PrimitiveTypeInfo)baseTypeInfo).getTypeName(), null);
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
    return structFields;
  }


  @Override
  public int hashCode(){
    return baseTypeInfo.hashCode();
    // true, we have other fields here, but all other fields base themselves off parsing
    // the base TypeInfo. Also, for equal HowlTypeInfos, the hashCode() must be the same
  }

  @Override
  public boolean equals(Object other){
    if (other == null){
      return false;
      // no need to check if we're null too, because we disallow empty ctor,
      // and baseTypeInfo *will* be set on instantiations. if baseTypeInfo is
      // not set because of some future modification without this being changed,
      // it's good to throw that exception
    }
    return baseTypeInfo.equals(((HowlTypeInfo)other).baseTypeInfo);
  }

  /**
   * @return the type
   */
  public HowlType getType() {
    return type;
  }

  /**
   * @return the string representation of the type
   */
  public String getTypeString(){
    return baseTypeInfo.getTypeName();
  }

  /**
   * package scope function - returns the underlying TypeInfo
   * @return the underlying TypeInfo
   */
  TypeInfo getTypeInfo(){
    return baseTypeInfo;
  }

}
