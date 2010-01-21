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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName = "Alter Table")
public class alterTableDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static enum alterTableTypes {
    RENAME, ADDCOLS, REPLACECOLS, ADDPROPS, ADDSERDE, ADDSERDEPROPS, ADDFILEFORMAT, ADDCLUSTERSORTCOLUMN, RENAMECOLUMN
  };

  alterTableTypes op;
  String oldName;
  String newName;
  List<FieldSchema> newCols;
  String serdeName;
  Map<String, String> props;
  String inputFormat;
  String outputFormat;
  int numberBuckets;
  List<String> bucketColumns;
  List<Order> sortColumns;

  String oldColName;
  String newColName;
  String newColType;
  String newColComment;
  boolean first;
  String afterCol;

  /**
   * @param tblName
   *          table name
   * @param oldColName
   *          old column name
   * @param newColName
   *          new column name
   * @param newComment
   * @param newType
   */
  public alterTableDesc(String tblName, String oldColName, String newColName,
      String newType, String newComment, boolean first, String afterCol) {
    super();
    oldName = tblName;
    this.oldColName = oldColName;
    this.newColName = newColName;
    newColType = newType;
    newColComment = newComment;
    this.first = first;
    this.afterCol = afterCol;
    op = alterTableTypes.RENAMECOLUMN;
  }

  /**
   * @param oldName
   *          old name of the table
   * @param newName
   *          new name of the table
   */
  public alterTableDesc(String oldName, String newName) {
    op = alterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
  }

  /**
   * @param name
   *          name of the table
   * @param newCols
   *          new columns to be added
   */
  public alterTableDesc(String name, List<FieldSchema> newCols,
      alterTableTypes alterType) {
    op = alterType;
    oldName = name;
    this.newCols = newCols;
  }

  /**
   * @param alterType
   *          type of alter op
   */
  public alterTableDesc(alterTableTypes alterType) {
    op = alterType;
  }

  /**
   * 
   * @param name
   *          name of the table
   * @param inputFormat
   *          new table input format
   * @param outputFormat
   *          new table output format
   */
  public alterTableDesc(String name, String inputFormat, String outputFormat,
      String serdeName) {
    super();
    op = alterTableTypes.ADDFILEFORMAT;
    oldName = name;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serdeName = serdeName;
  }

  public alterTableDesc(String tableName, int numBuckets,
      List<String> bucketCols, List<Order> sortCols) {
    oldName = tableName;
    op = alterTableTypes.ADDCLUSTERSORTCOLUMN;
    numberBuckets = numBuckets;
    bucketColumns = bucketCols;
    sortColumns = sortCols;
  }

  /**
   * @return the old name of the table
   */
  @explain(displayName = "old name")
  public String getOldName() {
    return oldName;
  }

  /**
   * @param oldName
   *          the oldName to set
   */
  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  /**
   * @return the newName
   */
  @explain(displayName = "new name")
  public String getNewName() {
    return newName;
  }

  /**
   * @param newName
   *          the newName to set
   */
  public void setNewName(String newName) {
    this.newName = newName;
  }

  /**
   * @return the op
   */
  public alterTableTypes getOp() {
    return op;
  }

  @explain(displayName = "type")
  public String getAlterTableTypeString() {
    switch (op) {
    case RENAME:
      return "rename";
    case ADDCOLS:
      return "add columns";
    case REPLACECOLS:
      return "replace columns";
    }

    return "unknown";
  }

  /**
   * @param op
   *          the op to set
   */
  public void setOp(alterTableTypes op) {
    this.op = op;
  }

  /**
   * @return the newCols
   */
  public List<FieldSchema> getNewCols() {
    return newCols;
  }

  @explain(displayName = "new columns")
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }

  /**
   * @param newCols
   *          the newCols to set
   */
  public void setNewCols(List<FieldSchema> newCols) {
    this.newCols = newCols;
  }

  /**
   * @return the serdeName
   */
  @explain(displayName = "deserializer library")
  public String getSerdeName() {
    return serdeName;
  }

  /**
   * @param serdeName
   *          the serdeName to set
   */
  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  /**
   * @return the props
   */
  @explain(displayName = "properties")
  public Map<String, String> getProps() {
    return props;
  }

  /**
   * @param props
   *          the props to set
   */
  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /**
   * @return the input format
   */
  @explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  /**
   * @param inputFormat
   *          the input format to set
   */
  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  /**
   * @return the output format
   */
  @explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  /**
   * @param outputFormat
   *          the output format to set
   */
  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  /**
   * @return the number of buckets
   */
  public int getNumberBuckets() {
    return numberBuckets;
  }

  /**
   * @param numberBuckets
   *          the number of buckets to set
   */
  public void setNumberBuckets(int numberBuckets) {
    this.numberBuckets = numberBuckets;
  }

  /**
   * @return the bucket columns
   */
  public List<String> getBucketColumns() {
    return bucketColumns;
  }

  /**
   * @param bucketColumns
   *          the bucket columns to set
   */
  public void setBucketColumns(List<String> bucketColumns) {
    this.bucketColumns = bucketColumns;
  }

  /**
   * @return the sort columns
   */
  public List<Order> getSortColumns() {
    return sortColumns;
  }

  /**
   * @param sortColumns
   *          the sort columns to set
   */
  public void setSortColumns(List<Order> sortColumns) {
    this.sortColumns = sortColumns;
  }

  /**
   * @return old column name
   */
  public String getOldColName() {
    return oldColName;
  }

  /**
   * @param oldColName
   *          the old column name
   */
  public void setOldColName(String oldColName) {
    this.oldColName = oldColName;
  }

  /**
   * @return new column name
   */
  public String getNewColName() {
    return newColName;
  }

  /**
   * @param newColName
   *          the new column name
   */
  public void setNewColName(String newColName) {
    this.newColName = newColName;
  }

  /**
   * @return new column type
   */
  public String getNewColType() {
    return newColType;
  }

  /**
   * @param newType
   *          new column's type
   */
  public void setNewColType(String newType) {
    newColType = newType;
  }

  /**
   * @return new column's comment
   */
  public String getNewColComment() {
    return newColComment;
  }

  /**
   * @param newComment
   *          new column's comment
   */
  public void setNewColComment(String newComment) {
    newColComment = newComment;
  }

  /**
   * @return if the column should be changed to position 0
   */
  public boolean getFirst() {
    return first;
  }

  /**
   * @param first
   *          set the column to position 0
   */
  public void setFirst(boolean first) {
    this.first = first;
  }

  /**
   * @return the column's after position
   */
  public String getAfterCol() {
    return afterCol;
  }

  /**
   * @param afterCol
   *          set the column's after position
   */
  public void setAfterCol(String afterCol) {
    this.afterCol = afterCol;
  }

}
