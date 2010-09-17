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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.schema.HFieldSchema.Type;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


public class HowlSchemaUtils {

    private static HowlSchemaUtils ref = new HowlSchemaUtils();

    public static CollectionBuilder getStructSchemaBuilder(){
        return ref.new CollectionBuilder();
    }

    public static MapBuilder getMapSchemaBuilder(){
        return ref.new MapBuilder();
    }


    public abstract class HSchemaBuilder {
        public abstract HSchema build() throws HowlException;
    }

    public class CollectionBuilder extends HSchemaBuilder { // for STRUCTS(multiple-add-calls) and LISTS(single-add-call)
        List<HFieldSchema> fieldSchemas = null;

        CollectionBuilder(){
            fieldSchemas = new ArrayList<HFieldSchema>();
        }

        public CollectionBuilder addField(FieldSchema fieldSchema) throws HowlException{
            return this.addField(getHFieldSchema(fieldSchema));
        }
        
        public CollectionBuilder addField(HFieldSchema fieldColumnSchema){
            fieldSchemas.add(fieldColumnSchema);
            return this;
        }

        @Override
        public HSchema build() throws HowlException{
            return new HSchema(fieldSchemas);
        }

    }

    public class MapBuilder extends HSchemaBuilder {

        Type keyType = null;
        HSchema valueSchema = null;

        @Override
        public HSchema build() throws HowlException {
            List<HFieldSchema> fslist = new ArrayList<HFieldSchema>();
            fslist.add(new HFieldSchema(null,Type.MAP,keyType,valueSchema));
            return new HSchema(fslist);
        }

        public MapBuilder withValueSchema(HSchema valueSchema) {
            this.valueSchema = valueSchema;
            return this;
        }

        public MapBuilder withKeyType(Type keyType) {
            this.keyType = keyType;
            return this;
        }

    }


    /**
     * Convert a HFieldSchema to a FieldSchema
     * @param fs FieldSchema to convert
     * @return HFieldSchema representation of FieldSchema
     * @throws HowlException 
     */
    public static HFieldSchema getHFieldSchema(FieldSchema fs) throws HowlException {
        String fieldName = fs.getName();
        TypeInfo baseTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fs.getType());
        return getHFieldSchema(fieldName, baseTypeInfo);
    }

    private static HFieldSchema getHFieldSchema(String fieldName, TypeInfo fieldTypeInfo) throws HowlException {
        Category typeCategory = fieldTypeInfo.getCategory();
        if (Category.PRIMITIVE == typeCategory){
            return new HFieldSchema(fieldName,getPrimitiveHType(fieldTypeInfo));
        } else if (Category.STRUCT == typeCategory) {
            HSchema subSchema = getHowlSchema(((StructTypeInfo)fieldTypeInfo));
            return new HFieldSchema(fieldName,HFieldSchema.Type.STRUCT,subSchema);
        } else if (Category.LIST == typeCategory) {
            HSchema subSchema = getHowlSchema(((ListTypeInfo)fieldTypeInfo).getListElementTypeInfo());
            return new HFieldSchema(fieldName,HFieldSchema.Type.ARRAY,subSchema);
        } else if (Category.MAP == typeCategory) {
            HFieldSchema.Type mapKeyType =  getPrimitiveHType(((MapTypeInfo)fieldTypeInfo).getMapKeyTypeInfo());
            HSchema subSchema = getHowlSchema(((MapTypeInfo)fieldTypeInfo).getMapValueTypeInfo());
            return new HFieldSchema(fieldName,HFieldSchema.Type.MAP,mapKeyType,subSchema);
        } else{
            throw new TypeNotPresentException(fieldTypeInfo.getTypeName(),null);
        }
    }

    private static Type getPrimitiveHType(TypeInfo basePrimitiveTypeInfo) {
        switch(((PrimitiveTypeInfo)basePrimitiveTypeInfo).getPrimitiveCategory()) {
        case BOOLEAN:
            return HFieldSchema.Type.BOOLEAN;
        case BYTE:
            return HFieldSchema.Type.TINYINT;
        case DOUBLE:
            return HFieldSchema.Type.DOUBLE;
        case FLOAT:
            return HFieldSchema.Type.FLOAT;
        case INT:
            return HFieldSchema.Type.INT;
        case LONG:
            return HFieldSchema.Type.BIGINT;
        case SHORT:
            return HFieldSchema.Type.SMALLINT;
        case STRING:
            return HFieldSchema.Type.STRING;
        default:
            throw new TypeNotPresentException(((PrimitiveTypeInfo)basePrimitiveTypeInfo).getTypeName(), null);
        }
    }

    public static HSchema getHowlSchema(Schema schema) throws HowlException{
        return getHowlSchema(schema.getFieldSchemas());
    }

    public static HSchema getHowlSchema(List<? extends FieldSchema> fslist) throws HowlException{
        CollectionBuilder builder = getStructSchemaBuilder();
        for (FieldSchema fieldSchema : fslist){
            builder.addField(fieldSchema);
        }
        return builder.build();
    }

    public static HSchema getHowlSchema(TypeInfo typeInfo) throws HowlException {
        Category typeCategory = typeInfo.getCategory();
        if (Category.PRIMITIVE == typeCategory){
            return getStructSchemaBuilder().addField(new HFieldSchema(null,getPrimitiveHType(typeInfo))).build();
        } else if (Category.STRUCT == typeCategory) {
            CollectionBuilder builder = getStructSchemaBuilder();
            for (String fieldName : ((StructTypeInfo)typeInfo).getAllStructFieldNames()){
                builder.addField(getHFieldSchema(fieldName,((StructTypeInfo)typeInfo).getStructFieldTypeInfo(fieldName)));
            }
            return builder.build();
        } else if (Category.LIST == typeCategory) {
            CollectionBuilder builder = getStructSchemaBuilder();
            builder.addField(getHFieldSchema(null,((ListTypeInfo)typeInfo).getListElementTypeInfo()));
            return builder.build();
        } else if (Category.MAP == typeCategory) {
            HFieldSchema.Type mapKeyType =  getPrimitiveHType(((MapTypeInfo)typeInfo).getMapKeyTypeInfo());
            HSchema subSchema = getHowlSchema(((MapTypeInfo)typeInfo).getMapValueTypeInfo());
            MapBuilder builder = getMapSchemaBuilder();
            return builder.withKeyType(mapKeyType).withValueSchema(subSchema).build();
        } else{
            throw new TypeNotPresentException(typeInfo.getTypeName(),null);
        }
    }

    public static HSchema getHowlSchema(String typeString) throws HowlException {
        return getHowlSchema(TypeInfoUtils.getTypeInfoFromTypeString(typeString));
    }

}
