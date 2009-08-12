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

package org.apache.hadoop.hive.ql.plan;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;

import org.apache.thrift.protocol.TBinaryProtocol;

public class PlanUtils {

  public static enum ExpressionTypes {FIELD, JEXL};

  @SuppressWarnings("nls")
  public static mapredWork getMapRedWork() {
    return new mapredWork("", 
                          new LinkedHashMap<String, ArrayList<String>> (),
                          new LinkedHashMap<String, partitionDesc> (),
                          new LinkedHashMap<String, Operator<? extends Serializable>> (),
                          new tableDesc(),
                          new ArrayList<tableDesc> (),
                          null,
                          Integer.valueOf (1), null);
  }
  
  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   * and column names (comma separated string).
   */
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns) {
    return getDefaultTableDesc(separatorCode, columns, false);
  }

  /** 
   * Generate the table descriptor of given serde with the separatorCode
   * and column names (comma separated string).
   */
  public static tableDesc getTableDesc(Class<? extends Deserializer> serdeClass, String separatorCode, String columns) {
    return getTableDesc(serdeClass, separatorCode, columns, false);
  }

  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   * and column names (comma separated string), and whether the last column should take
   * the rest of the line.
   */
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns,
      boolean lastColumnTakesRestOfTheLine) {
    return getDefaultTableDesc(separatorCode, columns, null, lastColumnTakesRestOfTheLine);
  }

  /** 
   * Generate the table descriptor of the serde specified with the separatorCode
   * and column names (comma separated string), and whether the last column should take
   * the rest of the line.
   */
  public static tableDesc getTableDesc(Class<? extends Deserializer> serdeClass, 
                                       String separatorCode, String columns,
                                       boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(serdeClass, separatorCode, columns, null, lastColumnTakesRestOfTheLine);
  }

  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   * and column names (comma separated string), and whether the last column should take
   * the rest of the line.
   */
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns, String columnTypes,
      boolean lastColumnTakesRestOfTheLine) {
    return getTableDesc(LazySimpleSerDe.class, separatorCode, columns, columnTypes,
                        lastColumnTakesRestOfTheLine);
  }

  public static tableDesc getTableDesc(Class<? extends Deserializer> serdeClass,
                                       String separatorCode, String columns, String columnTypes,
      boolean lastColumnTakesRestOfTheLine) {
    Properties properties = Utilities.makeProperties(
      Constants.SERIALIZATION_FORMAT, separatorCode,
      Constants.LIST_COLUMNS, columns);
    if (columnTypes != null)
      properties.setProperty(Constants.LIST_COLUMN_TYPES, columnTypes);
    
    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(
          Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }
    return new tableDesc(
      serdeClass,
      TextInputFormat.class,
      IgnoreKeyTextOutputFormat.class,
      properties);    
  }
  
  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode.
   * MetaDataTypedColumnsetSerDe is used because LazySimpleSerDe does not support a table
   * with a single column "col" with type "array<string>".
   */
  public static tableDesc getDefaultTableDesc(String separatorCode) {
    return new tableDesc(
        MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class,
        Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, separatorCode));    
  }

  /** 
   * Generate the table descriptor for reduce key.
   */
  public static tableDesc getReduceKeyTableDesc(List<FieldSchema> fieldSchemas, String order) {
    return new tableDesc(
        BinarySortableSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            Constants.LIST_COLUMNS,
              MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES, 
              MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.SERIALIZATION_SORT_ORDER, 
              order
        ));
  }

  /** 
   * Generate the table descriptor for Map-side join key.
   */
  public static tableDesc getMapJoinKeyTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(
        LazySimpleSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            "columns", MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            "columns.types", MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"
        ));
  }

  /** 
   * Generate the table descriptor for Map-side join key.
   */
  public static tableDesc getMapJoinValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(
        LazySimpleSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            "columns", MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            "columns.types", MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR, "\\"
        ));
  }

  /** 
   * Generate the table descriptor for intermediate files.
   */
  public static tableDesc getIntermediateFileTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(
        LazySimpleSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            Constants.LIST_COLUMNS,
              MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES,
              MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR,
              "\\"              
        ));
  }
  
  /** 
   * Generate the table descriptor for intermediate files.
   */
  public static tableDesc getReduceValueTableDesc(List<FieldSchema> fieldSchemas) {
    return new tableDesc(
        LazySimpleSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            Constants.LIST_COLUMNS,
              MetaStoreUtils.getColumnNamesFromFieldSchema(fieldSchemas),
            Constants.LIST_COLUMN_TYPES,
              MetaStoreUtils.getColumnTypesFromFieldSchema(fieldSchemas),
            Constants.ESCAPE_CHAR,
              "\\"              
        ));
  }
  
  /** 
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(List<exprNodeDesc> cols, List<String> outputColumnNames, int start,
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i=0; i<cols.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix + outputColumnNames.get(i+start), cols.get(i).getTypeInfo()));
    }
    return schemas;
  }
  
  /** 
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(List<exprNodeDesc> cols, 
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i=0; i<cols.size(); i++) {
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(fieldPrefix + i, cols.get(i).getTypeInfo()));
    }
    return schemas;
  }
  
  /** 
   * Convert the RowSchema to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromRowSchema(RowSchema row, String fieldPrefix) {
    Vector<ColumnInfo> c = row.getSignature();
    return getFieldSchemasFromColumnInfo(c, fieldPrefix);
  }
  
  /** 
   * Convert the ColumnInfo to FieldSchema.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnInfo(Vector<ColumnInfo> cols, String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i=0; i<cols.size(); i++) {
      String name = cols.get(i).getInternalName();
      if (name.equals(Integer.valueOf(i).toString())) {
        name = fieldPrefix + name; 
      }
      schemas.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(name, cols.get(i).getType()));
    }
    return schemas;
  }
  
  public static List<FieldSchema> sortFieldSchemas(List<FieldSchema> schema) {
    Collections.sort(schema, new Comparator<FieldSchema>(){

      @Override
      public int compare(FieldSchema o1, FieldSchema o2) {
        return o1.getName().compareTo(o2.getName());
      }
      
    });
    return schema;
  }
  
  /**
   * Create the reduce sink descriptor.
   * @param keyCols   The columns to be stored in the key
   * @param valueCols The columns to be stored in the value
   * @param outputColumnNames The output columns names
   * @param tag       The tag for this reducesink
   * @param partitionCols The columns for partitioning.
   * @param numReducers  The number of reducers, set to -1 for automatic inference 
   *                     based on input data size.
   * @return The reduceSinkDesc object.
   */
  public static reduceSinkDesc getReduceSinkDesc(ArrayList<exprNodeDesc> keyCols, 
                                                 ArrayList<exprNodeDesc> valueCols,
                                                 List<String> outputColumnNames,
                                                 boolean includeKeyCols,
                                                 int tag, 
                                                 ArrayList<exprNodeDesc> partitionCols,
                                                 String order,
                                                 int numReducers) {
    tableDesc keyTable = null;
    tableDesc valueTable = null;
    ArrayList<String> outputKeyCols = new ArrayList<String>();
    ArrayList<String> outputValCols = new ArrayList<String>();
    if (includeKeyCols) {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(
          keyCols, outputColumnNames, 0, ""), order);
      outputKeyCols.addAll(outputColumnNames.subList(0, keyCols.size()));
      valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
          valueCols, outputColumnNames, keyCols.size(), ""));
      outputValCols.addAll(outputColumnNames.subList(keyCols.size(), outputColumnNames.size()));
    } else {
      keyTable = getReduceKeyTableDesc(getFieldSchemasFromColumnList(
          keyCols, "reducesinkkey"), order);
      for (int i = 0; i < keyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey"+i);
      }
      valueTable = getReduceValueTableDesc(getFieldSchemasFromColumnList(
          valueCols, outputColumnNames, 0, ""));
      outputValCols.addAll(outputColumnNames);
    }
    return new reduceSinkDesc(keyCols, valueCols, outputKeyCols, outputValCols, tag, partitionCols, numReducers, 
        keyTable,
        // Revert to DynamicSerDe: getBinaryTableDesc(getFieldSchemasFromColumnList(valueCols, "reducesinkvalue")));
        valueTable);
  }

  /**
   * Create the reduce sink descriptor.
   * @param keyCols   The columns to be stored in the key
   * @param valueCols The columns to be stored in the value
   * @param outputColumnNames The output columns names
   * @param tag       The tag for this reducesink
   * @param numPartitionFields  The first numPartitionFields of keyCols will be partition columns.
   *                  If numPartitionFields=-1, then partition randomly.
   * @param numReducers  The number of reducers, set to -1 for automatic inference 
   *                     based on input data size.
   * @return The reduceSinkDesc object.
   */
  public static reduceSinkDesc getReduceSinkDesc(
      ArrayList<exprNodeDesc> keyCols, ArrayList<exprNodeDesc> valueCols,
      List<String> outputColumnNames, boolean includeKey, int tag, int numPartitionFields,
      int numReducers) {
    ArrayList<exprNodeDesc> partitionCols = null;

    if (numPartitionFields >= keyCols.size()) {
      partitionCols = keyCols;
    } else if (numPartitionFields >= 0) {
      partitionCols = new ArrayList<exprNodeDesc>(numPartitionFields);
      for (int i=0; i<numPartitionFields; i++) {
        partitionCols.add(keyCols.get(i));
      }
    } else {
      // numPartitionFields = -1 means random partitioning
      partitionCols = new ArrayList<exprNodeDesc>(1);
      partitionCols.add(TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"));
    }
    
    StringBuilder order = new StringBuilder();
    for (int i=0; i<keyCols.size(); i++) {
      order.append("+");
    }
    return getReduceSinkDesc(keyCols, valueCols, outputColumnNames, includeKey, tag, partitionCols, order.toString(),
         numReducers);
  }

}
  
