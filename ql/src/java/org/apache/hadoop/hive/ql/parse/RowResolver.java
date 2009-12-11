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

package org.apache.hadoop.hive.ql.parse;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;

/**
 * Implementation of the Row Resolver
 *
 **/

public class RowResolver {

  private RowSchema rowSchema;
  private HashMap<String, LinkedHashMap<String, ColumnInfo>> rslvMap;

  private HashMap<String, String[]> invRslvMap;

  // TODO: Refactor this and do in a more object oriented manner
  private boolean isExprResolver;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(RowResolver.class.getName());
  
  public RowResolver() {
    rowSchema = new RowSchema();
    rslvMap = new HashMap<String, LinkedHashMap<String, ColumnInfo>>();
    invRslvMap = new HashMap<String, String[]>();
    isExprResolver = false;
  }
  
  public void put(String tab_alias, String col_alias, ColumnInfo colInfo) {
    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
    }
    col_alias = col_alias.toLowerCase();
    if (rowSchema.getSignature() == null) {
      rowSchema.setSignature(new Vector<ColumnInfo>());
    }

    rowSchema.getSignature().add(colInfo);

    LinkedHashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
    if (f_map == null) {
      f_map = new LinkedHashMap<String, ColumnInfo>();
      rslvMap.put(tab_alias, f_map);
    }
    f_map.put(col_alias, colInfo);

    String [] qualifiedAlias = new String[2];
    qualifiedAlias[0] = tab_alias;
    qualifiedAlias[1] = col_alias;
    invRslvMap.put(colInfo.getInternalName(), qualifiedAlias);
  }

  public boolean hasTableAlias(String tab_alias) {
    return rslvMap.get(tab_alias.toLowerCase()) != null;
  }

  /**
   * Gets the column Info to tab_alias.col_alias type of a column reference. I the tab_alias is not
   * provided as can be the case with an non aliased column, this function looks up the column in all
   * the table aliases in  this row resolver and returns the match. It also throws an exception if 
   * the column is found in multiple table aliases. If no match is found a null values is returned.
   * 
   * This allows us to interpret both select t.c1 type of references and select c1 kind of refereneces.
   * The later kind are what we call non aliased column references in the query.
   * 
   * @param tab_alias The table alias to match (this is null if the column reference is non aliased)
   * @param col_alias The column name that is being searched for
   * @return ColumnInfo
   * @throws SemanticException
   */
  public ColumnInfo get(String tab_alias, String col_alias) 
    throws SemanticException {
    col_alias = col_alias.toLowerCase();
    ColumnInfo ret = null;

    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
      HashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
      if (f_map == null) {
        return null;
      }
      ret = f_map.get(col_alias);
    }
    else {
      boolean found = false;
      for(LinkedHashMap<String, ColumnInfo> cmap: rslvMap.values()) {
        for(Map.Entry<String, ColumnInfo> cmapEnt: cmap.entrySet()) {
          if (col_alias.equalsIgnoreCase((String)cmapEnt.getKey())) {
            if (found) {
              throw new SemanticException("Column " + col_alias + " Found in more than One Tables/Subqueries");
            }
            found = true;
            ret = (ColumnInfo)cmapEnt.getValue();
          }
        }
      }
    }

    return ret; 
  }

  public Vector<ColumnInfo> getColumnInfos() {
    return rowSchema.getSignature();
  }
 
  public HashMap<String, ColumnInfo> getFieldMap(String tab_alias) {
    if (tab_alias == null) {
      return rslvMap.get(null);
    } else {
      return rslvMap.get(tab_alias.toLowerCase());
    }
  }

  public int getPosition(String internalName) {
    int pos = -1;

    for(ColumnInfo var: rowSchema.getSignature()) {
      ++pos;
      if (var.getInternalName().equals(internalName)) {
         return pos;
      }
    }

    return -1;
  }

  public Set<String> getTableNames() {
    return rslvMap.keySet();
  }

  public String[] reverseLookup(String internalName) {
    return invRslvMap.get(internalName);
  }
 
  public void setIsExprResolver(boolean isExprResolver) {
    this.isExprResolver = isExprResolver;
  }

  public boolean getIsExprResolver() {
    return isExprResolver;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    
    for(Map.Entry<String, LinkedHashMap<String,ColumnInfo>> e: rslvMap.entrySet()) {
      String tab = (String)e.getKey();
      sb.append(tab + "{");
      HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>)e.getValue();
      if (f_map != null)
        for(Map.Entry<String, ColumnInfo> entry: f_map.entrySet()) {
          sb.append("(" + (String)entry.getKey() + "," + entry.getValue().toString() + ")");
        }
      sb.append("} ");
    }
    return sb.toString();
  }
}
