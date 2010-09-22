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

import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

/**
 * Table Scan Descriptor Currently, data is only read from a base source as part
 * of map-reduce framework. So, nothing is stored in the descriptor. But, more
 * things will be added here as table scan is invoked as part of local work.
 **/
@Explain(displayName = "TableScan")
public class TableScanDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String alias;
  
  private List<VirtualColumn> virtualCols;

  private ExprNodeDesc filterExpr;

  public static final String FILTER_EXPR_CONF_STR =
    "hive.io.filter.expr.serialized";

  public static final String FILTER_TEXT_CONF_STR =
    "hive.io.filter.text";

  @SuppressWarnings("nls")
  public TableScanDesc() {
  }

  public TableScanDesc(final String alias) {
    this.alias = alias;
  }
  
  public TableScanDesc(final String alias, List<VirtualColumn> vcs) {
    this.alias = alias;
    this.virtualCols = vcs;
  }

  @Explain(displayName = "alias")
  public String getAlias() {
    return alias;
  }

  @Explain(displayName = "filterExpr")
  public ExprNodeDesc getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(ExprNodeDesc filterExpr) {
    this.filterExpr = filterExpr;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public List<VirtualColumn> getVirtualCols() {
    return virtualCols;
  }

  public void setVirtualCols(List<VirtualColumn> virtualCols) {
    this.virtualCols = virtualCols;
  }

}
