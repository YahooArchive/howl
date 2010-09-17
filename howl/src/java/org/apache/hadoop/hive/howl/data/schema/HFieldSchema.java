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
package org.apache.hadoop.hive.howl.data.schema;

import java.io.Serializable;

import org.apache.hadoop.hive.howl.common.HowlException;

public class HFieldSchema implements Serializable {

    public enum Type {
        INT,
        TINYINT,
        SMALLINT,
        BIGINT,
        BOOLEAN,
        FLOAT,
        DOUBLE,
        STRING,
        ARRAY,
        MAP,
        STRUCT,
    }

    public enum Category {
        PRIMITIVE,
        ARRAY,
        MAP,
        STRUCT;

        public static Category fromType(Type type) {
            if (Type.ARRAY == type){
                return ARRAY;
            }else if(Type.STRUCT == type){
                return STRUCT;
            }else if (Type.MAP == type){
                return MAP;
            }else{
                return PRIMITIVE;
            }
        }
    };
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    String fieldName = null;
    Type type = null;
    Category category = null;
    
    // Populated if column is struct, array or map types. 
    // If struct type, contains schema of the struct. 
    // If array type, contains schema of one of the elements.
    // If map type, contains schema of the value element.
    HSchema subSchema = null;
    
    // populated if column is Map type
    Type mapKeyType = null;

    @SuppressWarnings("unused")
    private HFieldSchema(){
        // preventing empty ctor from being callable
    }

    /**
     * Returns type of the field
     * @return type of the field
     */
    public Type getType(){
        return type;
    }
    
    /**
     * Returns category of the field
     * @return category of the field
     */
    public Category getCategory(){
        return category;
    }

    /**
     * Returns name of the field
     * @return name of the field
     */
    public String getName(){
        return fieldName;
    }

    /**
     * Constructor constructing a primitive datatype HFieldSchema
     * @param fieldName Name of the primitive field
     * @param type Type of the primitive field
     * @throws HowlException if call made on non-primitive types
     */
    public HFieldSchema(String fieldName, Type type) throws HowlException {
        assertTypeInCategory(type,Category.PRIMITIVE);
        this.fieldName = fieldName;
        this.type = type;
        this.category = Category.PRIMITIVE;
    }

    /**
     * Constructor for constructing a ARRAY type or STRUCT type HFieldSchema, passing type and subschema
     * @param fieldName Name of the array or struct field
     * @param type Type of the field - either Type.ARRAY or Type.STRUCT
     * @param subSchema - subschema of the struct, or element schema of the elements in the array
     * @throws HowlException if call made on Primitive or Map types
     */
    public HFieldSchema(String fieldName, Type type, HSchema subSchema) throws HowlException{
        assertTypeNotInCategory(type,Category.PRIMITIVE);
        assertTypeNotInCategory(type,Category.MAP);
        this.fieldName = fieldName;
        this.type = type;
        this.category = Category.fromType(type);
        this.subSchema = subSchema;
    }

    /**
     * Constructor for constructing a MAP type HFieldSchema, passing type of key and value
     * @param fieldName Name of the array or struct field
     * @param type Type of the field - must be Type.MAP
     * @param mapKeyType - key type of the Map
     * @param mapValueSchema - subschema of the value of the Map
     * @throws HowlException if call made on non-Map types
     */
    public HFieldSchema(String fieldName, Type type, Type mapKeyType, HSchema mapValueSchema) throws HowlException{
        assertTypeInCategory(type,Category.MAP);
        assertTypeInCategory(mapKeyType,Category.PRIMITIVE);
        this.fieldName = fieldName;
        this.type = Type.MAP;
        this.category = Category.MAP;
        this.mapKeyType = mapKeyType;
        this.subSchema = mapValueSchema;
    }

    public HSchema getStructSubSchema() throws HowlException {
        assertTypeInCategory(this.type,Category.STRUCT);
        return subSchema;
    }
    
    public HSchema getArrayElementSchema() throws HowlException {
        assertTypeInCategory(this.type,Category.ARRAY);
        return subSchema;
    }
    
    public Type getMapKeyType() throws HowlException {
        assertTypeInCategory(this.type,Category.MAP);
        return mapKeyType;
    }
    
    public HSchema getMapValueSchema() throws HowlException {
        assertTypeInCategory(this.type,Category.MAP);
        return subSchema;
    }

    private static void assertTypeInCategory(Type type, Category category) throws HowlException {
        Category typeCategory = Category.fromType(type);
        if (typeCategory != category){
            throw new HowlException("Type category mismatch. Expected "+category+" but type "+type+" in category "+typeCategory);
        }
    }

    private static void assertTypeNotInCategory(Type type, Category category) throws HowlException {
        Category typeCategory = Category.fromType(type);
        if (typeCategory == category){
            throw new HowlException("Type category mismatch. Expected type "+type+" not in category "+category+" but was so.");
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        if (fieldName != null){
            sb.append(fieldName);
            sb.append(":");
        }
        if (Category.PRIMITIVE == category){
            sb.append(type);
        }else if (Category.STRUCT == category){
            sb.append("struct<");
            sb.append(subSchema.toString());
            sb.append(">");
        }else if (Category.ARRAY == category){
            sb.append("array<");
            sb.append(subSchema.toString());
            sb.append(">");
        }else if (Category.MAP == category){
            sb.append("map<");
            sb.append(mapKeyType);
            sb.append(",");
            sb.append(subSchema.toString());
            sb.append(">");
        }
        return sb.toString().toLowerCase();
    }
}
