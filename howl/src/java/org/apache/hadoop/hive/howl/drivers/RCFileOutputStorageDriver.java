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
package org.apache.hadoop.hive.howl.drivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver;
import org.apache.hadoop.hive.io.RCFileMapReduceOutputFormat;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * The storage driver for writing RCFile data through HowlOutputFormat.
 */
 public class RCFileOutputStorageDriver extends HowlOutputStorageDriver {

   /** The serde for serializing the HowlRecord to bytes writable */
   private SerDe serde;

   /** The object inspector for the given schema */
   private StructObjectInspector objectInspector;

   /** The schema for the output data */
   private Schema outputSchema;

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#convertValue(org.apache.hadoop.hive.howl.data.HowlRecord)
   */
  @Override
  public Writable convertValue(HowlRecord value) throws IOException {
    try {
      List<Object> objectList = new ArrayList<Object>();

      for(int i = 0;i < value.size();i++ ) {
        objectList.add(value.get(i));
      }

      return serde.serialize(objectList, objectInspector);
    } catch(SerDeException e) {
      throw new IOException(e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#generateKey(org.apache.hadoop.hive.howl.data.HowlRecord)
   */
  @Override
  public WritableComparable<?> generateKey(HowlRecord value) throws IOException {
    //key is not used for RCFile output
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#getOutputFormat(java.util.Properties)
   */
  @SuppressWarnings("unchecked")
  @Override
  public OutputFormat<? super WritableComparable<?>, ? super Writable> getOutputFormat() throws IOException {
    return (OutputFormat) new RCFileMapReduceOutputFormat();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setOutputPath(org.apache.hadoop.mapreduce.JobContext, java.lang.String)
   */
  @Override
  public void setOutputPath(JobContext jobContext, String location) throws IOException {
    //Not calling FileOutputFormat.setOutputPath since that requires a Job instead of JobContext
    jobContext.getConfiguration().set("mapred.output.dir", location);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setPartitionValues(org.apache.hadoop.mapreduce.JobContext, java.util.Map)
   */
  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {
    //default implementation of HowlOutputStorageDriver.getPartitionLocation will use the partition
    //values to generate the data location, so partition values not used here
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.howl.mapreduce.HowlOutputStorageDriver#setSchema(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.hive.metastore.api.Schema)
   */
  @Override
  public void setSchema(JobContext jobContext, Schema schema) throws IOException {
    outputSchema = schema;
  }

  @Override
  public void initialize(JobContext context,Properties howlProperties) throws IOException {

    super.initialize(context, howlProperties);

    howlProperties.setProperty(Constants.LIST_COLUMNS,
          MetaStoreUtils.getColumnNamesFromFieldSchema(outputSchema.getFieldSchemas()));
    howlProperties.setProperty(Constants.LIST_COLUMN_TYPES,
          MetaStoreUtils.getColumnTypesFromFieldSchema(outputSchema.getFieldSchemas()));

    try {
      serde = new ColumnarSerDe();
      serde.initialize(context.getConfiguration(), howlProperties);
      objectInspector = createStructObjectInspector();

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  public StructObjectInspector createStructObjectInspector() throws IOException {

    if( outputSchema == null ) {
      throw new IOException("Invalid output schema specified");
    }

    List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
    List<String> fieldNames = new ArrayList<String>();

    for(FieldSchema fieldSchema : outputSchema.getFieldSchemas()) {
      TypeInfo type = TypeInfoUtils.getTypeInfoFromTypeString(fieldSchema.getType());

      fieldNames.add(fieldSchema.getName());
      fieldInspectors.add(getObjectInspector(type));
    }

    StructObjectInspector structInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(fieldNames, fieldInspectors);
    return structInspector;
  }

  public ObjectInspector getObjectInspector(TypeInfo type) throws IOException {

    switch(type.getCategory()) {

    case PRIMITIVE :
      PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
      return PrimitiveObjectInspectorFactory.
        getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());

    case MAP :
      MapTypeInfo mapType = (MapTypeInfo) type;
      MapObjectInspector mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
          getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
      return mapInspector;

    case LIST :
      ListTypeInfo listType = (ListTypeInfo) type;
      ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(
          getObjectInspector(listType.getListElementTypeInfo()));
      return listInspector;

    case STRUCT :
      StructTypeInfo structType = (StructTypeInfo) type;
      List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

      List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
      for(TypeInfo fieldType : fieldTypes) {
        fieldInspectors.add(getObjectInspector(fieldType));
      }

      StructObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
          structType.getAllStructFieldNames(), fieldInspectors);
      return structInspector;

    default :
      throw new IOException("Unknown field schema type");
    }
  }

}
