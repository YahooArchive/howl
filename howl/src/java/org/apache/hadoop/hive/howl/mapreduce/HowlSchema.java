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
package org.apache.hadoop.hive.howl.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.pig.impl.util.ObjectSerializer;

/**
 * Class which represents a howlSchema in OwlBase.
 */
public class HowlSchema implements Serializable {

  public enum ColumnType implements Serializable {
    ANY("any"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double"),
    BOOL("bool"),
    COLLECTION("collection"),
    MAP("map"),
    RECORD("record"),
    STRING("string"),
    BYTES("bytes");

    private String name;

    private ColumnType(String name) {
      this.name = name;
    }

    /**
     * To get the type based on the type name string.
     * @param name name of the type
     * @return ColumnType Enum for the type
     */
    public static ColumnType getTypeByName(String name) {
      return ColumnType.valueOf(name.toUpperCase());
    }

    public static String findTypeName(ColumnType columntype) {
      return columntype.getName();
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }

    public static boolean isSchemaType(ColumnType columnType) {
      return ((columnType == RECORD) || (columnType == MAP) || (columnType == COLLECTION));
    }
  }

  /**
   * Class which represents a ColumnSchema in the HowlSchema.
   */
  public class ColumnSchema implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    /** The column name. */
    private String name;

    /** The column data type. */
    private ColumnType type;

    /** The subschema for the column. */
    private HowlSchema howlSchema;

    /** The description. */
    private String description;

    private Integer columnNumber = null;


    public ColumnSchema(){
    }
    /**
     * Initialize an ColumnSchema.
     * @param name column name
     * @param type column type
     * @throws Exception
     * @throws OwlException
     */
    public ColumnSchema(String name, ColumnType type) throws Exception {
      this(name, type, null);
    }

    /**
     * Initialize an ColumnSchema.
     * @param name column name
     * @param type the column type
     * @param howlSchema column howlSchema
     * @throws Exception
     * @throws OwlException the owl exception
     */
    public ColumnSchema(String name, ColumnType type, HowlSchema howlSchema ) throws Exception {

      if ((howlSchema != null) && !(ColumnType.isSchemaType(type))) {
        throw new Exception("Only a COLLECTION or RECORD or MAP can have schemas.");
      }

      this.name = name.toLowerCase();
      this.howlSchema = howlSchema;
      this.type = type;
    }

    /**
     * Initialize an ColumnSchema.
     * @param name column name
     * @param type the column type
     * @param howlSchema column howlSchema
     * @throws Exception
     * @throws OwlException the owl exception
     */
    public ColumnSchema(ColumnType type, HowlSchema howlSchema ) throws Exception  {

      if (type != ColumnType.RECORD){
        throw new Exception ("Missing columnname for howlSchema ["+ howlSchema.getSchemaString() +"]");
      }
      this.name = null;
      this.howlSchema = howlSchema;
      this.type = type;
    }

    /**
     * Gets the name.
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets the name.
     * @param name the new name
     */
    public void setName(String name) {
      this.name = name.toLowerCase();
    }

    /**
     * Gets the type.
     * @return the type
     */
    public ColumnType getType() {
      return type;
    }

    /**
     * Sets the type.
     * @param type the new type
     */
    public void setType(ColumnType type) {
      this.type = type;
    }

    /**
     * Gets the howlSchema.
     * @return the howlSchema
     */
    public HowlSchema getSchema() {
      return howlSchema;
    }

    /**
     * Sets the howlSchema.
     * @param howlSchema the new howlSchema
     */
    public void setSchema(HowlSchema howlSchema) {
      this.howlSchema = howlSchema;
    }

    /**
     * Gets the description.
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    /**
     * Sets the description.
     * @param description the new description
     */
    public void setDescription(String description) {
      this.description = description;
    }



    /**
     * Gets the columnNumber.
     * @return the columnNumber
     */
    public Integer getColumnNumber() {
      return columnNumber;
    }

    /**
     * Sets the columnNumber.
     * @param columnNumber the columnNumber
     */
    public void setColumnNumber(Integer columnNumber) {
      this.columnNumber = columnNumber;
    }

    // description is not included in hashCode() and equals()
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
      + ((columnNumber == null) ? 0 : columnNumber.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((howlSchema == null) ? 0 : howlSchema.hashCode());
      result = prime * result + ((type == null) ? 0 : type.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ColumnSchema other = (ColumnSchema) obj;
      if (columnNumber == null) {
        if (other.columnNumber != null) {
          return false;
        }
      } else if (!columnNumber.equals(other.columnNumber)) {
        return false;
      }
      if (name == null) {
        if (other.name != null) {
          return false;
        }
      } else if (!name.equals(other.name)) {
        return false;
      }
      if (howlSchema == null) {
        if (other.howlSchema != null) {
          return false;
        }
      } else if (!howlSchema.equals(other.howlSchema)) {
        return false;
      }
      if (type == null) {
        if (other.type != null) {
          return false;
        }
      } else if (!type.equals(other.type)) {
        return false;
      }
      return true;
    }

  }

  /** The serialization version */
  private static final long serialVersionUID = 1L;

  /** The identifier for the HowlSchema */
  private Integer schemaId;

  /** The list of ColumnSchema. */
  private ArrayList<ColumnSchema> columnSchema;

  /**
   * Instantiates a new owl howlSchema.
   */
  public HowlSchema() {
  }

  /**
   * Instantiates a new owl howlSchema.
   */
  public HowlSchema(ArrayList<ColumnSchema> columnSchema) {
    this.columnSchema = columnSchema;
  }

  /**
   * Instantiates a new owl howlSchema.
   */
  public HowlSchema(Integer schemaId, ArrayList<ColumnSchema> columnSchema) {
    this.schemaId = schemaId;
    this.columnSchema = columnSchema;
  }

  /**
   * Adds the given column howlSchema to the current howlSchema's list of columns
   * @param column the column howlSchema to add
   */
  public void addColumnSchema(ColumnSchema column) {
    if( columnSchema == null ) {
      columnSchema = new ArrayList<ColumnSchema>();
    }

    if( column.getColumnNumber() == null ) {
      column.setColumnNumber(columnSchema.size());
    }

    columnSchema.add(column);
  }

  /**
   * Gets the column count.
   * @return the column count
   */
  public int getColumnCount() {
    if( columnSchema == null ) {
      return 0;
    }

    return columnSchema.size();
  }

  /**
   * Gets the howlSchema for the column at given index.
   * @param index the index
   * @return the column howlSchema
   * @throws Exception
   */
  public ColumnSchema columnAt(int index) throws Exception  {
    if( columnSchema == null ) {
      throw new Exception("Column howlSchema not initialized");
    }else if( index < 0 || index >= columnSchema.size() ) {
      throw new Exception( "Invalid column index " + index);
    } else {
      return columnSchema.get(index);
    }
  }

  /**
   * Gets the value of columnSchema
   * @return the columnSchema
   */
  public ArrayList<ColumnSchema> getColumnSchema(){
    return columnSchema;
  }

  /**
   * Sort the list of ColumnSchema based on each column's positionNumber
   * @param columnSchema
   */
  public void setColumnSchema(ArrayList<ColumnSchema> owlColumnSchema){
    this.sortColumnSchemaList(owlColumnSchema);
    this.columnSchema = owlColumnSchema;
  }

  public void sortColumnSchemaList(ArrayList<ColumnSchema> owlColumnSchema){
    boolean hasColumnNumber = true;
    // sort recursively
    for(ColumnSchema column : owlColumnSchema ) {
      if ( column.getColumnNumber() != null){
        if (column.getSchema() !=  null) {
          // if has subschema
          HowlSchema s = column.getSchema();
          s.sortColumnSchemaList(s.getColumnSchema());
        }
      }else {
        hasColumnNumber = false;
      }
    }//for
    if (hasColumnNumber == true){
      Collections.sort(owlColumnSchema, new ColumnSchemaComparable());
    }
  }


  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result
    + ((columnSchema == null) ? 0 : columnSchema.hashCode());
    result = prime * result
    + ((schemaId == null) ? 0 : schemaId.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HowlSchema other = (HowlSchema) obj;
    if (columnSchema == null) {
      if (other.columnSchema != null) {
        return false;
      }
    } else if (!columnSchema.equals(other.columnSchema)) {
      return false;
    }
    if (schemaId == null) {
      if (other.schemaId != null) {
        return false;
      }
    } else if (!schemaId.equals(other.schemaId)) {
      return false;
    }
    return true;
  }

  /**
   * Gets the internal string representation of the howlSchema. String format is subject to change.
   * @return the howlSchema serialized string
   * @throws IOException
   */
  public String getSchemaString() throws IOException  {
    return ObjectSerializer.serialize(this);
  }

  public HowlSchema(String serializedSchema) throws IOException{
    this((HowlSchema) ObjectSerializer.deserialize(serializedSchema));
  }

  public HowlSchema(HowlSchema copy) {
    this.schemaId = copy.getSchemaId();
    this.columnSchema = copy.getColumnSchema();
  }

  /**
   * Sets the value of schemaId
   * @param schemaId the schemaId to set
   */
  public void setSchemaId(Integer schemaId) {
    this.schemaId = schemaId;
  }

  /**
   * Gets the value of schemaId
   * @return the schemaId
   */
  public Integer getSchemaId() {
    return schemaId;
  }

  public static class ColumnSchemaComparable implements Comparator<ColumnSchema>{
    //@Override
    public int compare(ColumnSchema c1, ColumnSchema c2) {
      int i1 = c1.getColumnNumber().intValue();
      int i2 = c2.getColumnNumber().intValue();
      return (i1 < i2 ? -1 : ((i1 == i2) ? 0 : 1));
    }
  }
}
