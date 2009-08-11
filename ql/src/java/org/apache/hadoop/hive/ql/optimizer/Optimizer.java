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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.ppd.PredicatePushDown;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcessor;

/**
 * Implementation of the optimizer
 */
public class Optimizer {
  private ParseContext pctx;
  private List<Transform> transformations;

	/**
	 * create the list of transformations
	 * @param hiveConf 
	 */
  public void initialize(HiveConf hiveConf) {
    transformations = new ArrayList<Transform>();
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCP)) {
      transformations.add(new ColumnPruner());
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD)) {
      transformations.add(new PredicatePushDown());
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPR)) {
        transformations.add(new PartitionPruner());
      }
    }
    transformations.add(new UnionProcessor());
    transformations.add(new MapJoinProcessor());
  }

  /**
   * invoke all the transformations one-by-one, and alter the query plan
   * @return ParseContext
   * @throws SemanticException
   */
  public ParseContext optimize() throws SemanticException {
    for (Transform t : transformations)
      pctx = t.transform(pctx);
    return pctx;
  }

  /**
   * @return the pctx
   */
  public ParseContext getPctx() {
    return pctx;
  }

  /**
   * @param pctx the pctx to set
   */
  public void setPctx(ParseContext pctx) {
    this.pctx = pctx;
  }

}
