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
package org.apache.howl.data.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.howl.common.HowlException;
import org.apache.howl.data.schema.HowlFieldSchema.Type;


public class HowlSchemaUtils {

    private static HowlSchemaUtils ref = new HowlSchemaUtils();

    public static CollectionBuilder getStructSchemaBuilder(){
        return ref.new CollectionBuilder();
    }

    public static CollectionBuilder getListSchemaBuilder(){
        return ref.new CollectionBuilder();
    }

    public static MapBuilder getMapSchemaBuilder(){
        return ref.new MapBuilder();
    }


    public abstract class HowlSchemaBuilder {
        public abstract HowlSchema build() throws HowlException;
    }

    public class CollectionBuilder extends HowlSchemaBuilder { // for STRUCTS(multiple-add-calls) and LISTS(single-add-call)
        List<HowlFieldSchema> fieldSchemas = null;

        CollectionBuilder(){
            fieldSchemas = new ArrayList<HowlFieldSchema>();
        }

        public CollectionBuilder addField(FieldSchema fieldSchema) throws HowlException{
            return this.addField(getHowlFieldSchema(fieldSchema));
        }

        public CollectionBuilder addField(HowlFieldSchema fieldColumnSchema){
            fieldSchemas.add(fieldColumnSchema);
            return this;
        }

        @Override
        public HowlSchema build() throws HowlException{
            return new HowlSchema(fieldSchemas);
        }

    }

    public class MapBuilder extends HowlSchemaBuilder {

        Type keyType = null;
        HowlSchema valueSchema = null;

        @Override
        public HowlSchema build() throws HowlException {
            List<HowlFieldSchema> fslist = new ArrayList<HowlFieldSchema>();
            fslist.add(new HowlFieldSchema(null,Type.MAP,keyType,valueSchema,null));
            return new HowlSchema(fslist);
        }

        public MapBuilder withValueSchema(HowlSchema valueSchema) {
            this.valueSchema = valueSchema;
            return this;
        }

        public MapBuilder withKeyType(Type keyType) {
            this.keyType = keyType;
            return this;
        }

    }


    /**
     * Convert a HowlFieldSchema to a FieldSchema
     * @param fs FieldSchema to convert
     * @return HowlFieldSchema representation of FieldSchema
     * @throws HowlException
     */
    public static HowlFieldSchema getHowlFieldSchema(FieldSchema fs) throws HowlException {
        String fieldName = fs.getName();
        TypeInfo baseTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fs.getType());
        return getHowlFieldSchema(fieldName, baseTypeInfo);
    }

    private static HowlFieldSchema getHowlFieldSchema(String fieldName, TypeInfo fieldTypeInfo) throws HowlException {
        Category typeCategory = fieldTypeInfo.getCategory();
        if (Category.PRIMITIVE == typeCategory){
            return new HowlFieldSchema(fieldName,getPrimitiveHType(fieldTypeInfo),null);
        } else if (Category.STRUCT == typeCategory) {
            HowlSchema subSchema = constructHowlSchema((StructTypeInfo)fieldTypeInfo);
            return new HowlFieldSchema(fieldName,HowlFieldSchema.Type.STRUCT,subSchema,null);
        } else if (Category.LIST == typeCategory) {
            HowlSchema subSchema = getHowlSchema(((ListTypeInfo)fieldTypeInfo).getListElementTypeInfo());
            return new HowlFieldSchema(fieldName,HowlFieldSchema.Type.ARRAY,subSchema,null);
        } else if (Category.MAP == typeCategory) {
            HowlFieldSchema.Type mapKeyType =  getPrimitiveHType(((MapTypeInfo)fieldTypeInfo).getMapKeyTypeInfo());
            HowlSchema subSchema = getHowlSchema(((MapTypeInfo)fieldTypeInfo).getMapValueTypeInfo());
            return new HowlFieldSchema(fieldName,HowlFieldSchema.Type.MAP,mapKeyType,subSchema,null);
        } else{
            throw new TypeNotPresentException(fieldTypeInfo.getTypeName(),null);
        }
    }

    private static Type getPrimitiveHType(TypeInfo basePrimitiveTypeInfo) {
        switch(((PrimitiveTypeInfo)basePrimitiveTypeInfo).getPrimitiveCategory()) {
        case BOOLEAN:
            return Type.BOOLEAN;
        case BYTE:
            return Type.TINYINT;
        case DOUBLE:
            return Type.DOUBLE;
        case FLOAT:
            return Type.FLOAT;
        case INT:
            return Type.INT;
        case LONG:
            return Type.BIGINT;
        case SHORT:
            return Type.SMALLINT;
        case STRING:
            return Type.STRING;
        default:
            throw new TypeNotPresentException(((PrimitiveTypeInfo)basePrimitiveTypeInfo).getTypeName(), null);
        }
    }

    public static HowlSchema getHowlSchema(Schema schema) throws HowlException{
        return getHowlSchema(schema.getFieldSchemas());
    }

    public static HowlSchema getHowlSchema(List<? extends FieldSchema> fslist) throws HowlException{
        CollectionBuilder builder = getStructSchemaBuilder();
        for (FieldSchema fieldSchema : fslist){
            builder.addField(fieldSchema);
        }
        return builder.build();
    }

    private static HowlSchema constructHowlSchema(StructTypeInfo stypeInfo) throws HowlException {
        CollectionBuilder builder = getStructSchemaBuilder();
        for (String fieldName : ((StructTypeInfo)stypeInfo).getAllStructFieldNames()){
            builder.addField(getHowlFieldSchema(fieldName,((StructTypeInfo)stypeInfo).getStructFieldTypeInfo(fieldName)));
        }
        return builder.build();
    }

    public static HowlSchema getHowlSchema(TypeInfo typeInfo) throws HowlException {
        Category typeCategory = typeInfo.getCategory();
        if (Category.PRIMITIVE == typeCategory){
            return getStructSchemaBuilder().addField(new HowlFieldSchema(null,getPrimitiveHType(typeInfo),null)).build();
        } else if (Category.STRUCT == typeCategory) {
            HowlSchema subSchema = constructHowlSchema((StructTypeInfo) typeInfo);
            return getStructSchemaBuilder().addField(new HowlFieldSchema(null,Type.STRUCT,subSchema,null)).build();
        } else if (Category.LIST == typeCategory) {
            CollectionBuilder builder = getStructSchemaBuilder();
            builder.addField(getHowlFieldSchema(null,((ListTypeInfo)typeInfo).getListElementTypeInfo()));
            return builder.build();
        } else if (Category.MAP == typeCategory) {
            HowlFieldSchema.Type mapKeyType =  getPrimitiveHType(((MapTypeInfo)typeInfo).getMapKeyTypeInfo());
            HowlSchema subSchema = getHowlSchema(((MapTypeInfo)typeInfo).getMapValueTypeInfo());
            MapBuilder builder = getMapSchemaBuilder();
            return builder.withKeyType(mapKeyType).withValueSchema(subSchema).build();
        } else{
            throw new TypeNotPresentException(typeInfo.getTypeName(),null);
        }
    }

    public static HowlSchema getHowlSchemaFromTypeString(String typeString) throws HowlException {
        return getHowlSchema(TypeInfoUtils.getTypeInfoFromTypeString(typeString));
    }

    public static HowlSchema getHowlSchema(String schemaString) throws HowlException {
        if ((schemaString == null) || (schemaString.trim().isEmpty())){
            return new HowlSchema(new ArrayList<HowlFieldSchema>()); // empty HSchema construct
        }
        HowlSchema outerSchema = getHowlSchemaFromTypeString("struct<"+schemaString+">");
        return outerSchema.get(0).getStructSubSchema();
    }

    public static FieldSchema getFieldSchema(HowlFieldSchema howlFieldSchema){
        return new FieldSchema(howlFieldSchema.getName(),howlFieldSchema.getTypeString(),howlFieldSchema.getComment());
    }

    public static List<FieldSchema> getFieldSchemas(List<HowlFieldSchema> howlFieldSchemas){
        List<FieldSchema> lfs = new ArrayList<FieldSchema>();
        for (HowlFieldSchema hfs : howlFieldSchemas){
            lfs.add(getFieldSchema(hfs));
        }
        return lfs;
    }
}
