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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * Filter operator implementation.
 **/
public class FilterOperator extends Operator<FilterDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    FILTERED, PASSED
  }

  private final transient LongWritable filtered_count, passed_count;
  private transient ExprNodeEvaluator conditionEvaluator;
  private transient PrimitiveObjectInspector conditionInspector;
  private transient int consecutiveFails;
  transient int heartbeatInterval;

  public FilterOperator() {
    super();
    filtered_count = new LongWritable();
    passed_count = new LongWritable();
    consecutiveFails = 0;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      heartbeatInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVESENDHEARTBEAT);
      conditionEvaluator = ExprNodeEvaluatorFactory.get(conf.getPredicate());
      statsMap.put(Counter.FILTERED, filtered_count);
      statsMap.put(Counter.PASSED, passed_count);
      conditionInspector = null;
    } catch (Throwable e) {
      throw new HiveException(e);
    }
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    ObjectInspector rowInspector = inputObjInspectors[tag];
    if (conditionInspector == null) {
      conditionInspector = (PrimitiveObjectInspector) conditionEvaluator
          .initialize(rowInspector);
    }
    Object condition = conditionEvaluator.evaluate(row);
    Boolean ret = (Boolean) conditionInspector
        .getPrimitiveJavaObject(condition);
    if (Boolean.TRUE.equals(ret)) {
      forward(row, rowInspector);
      passed_count.set(passed_count.get() + 1);
      consecutiveFails = 0;
    } else {
      filtered_count.set(filtered_count.get() + 1);
      consecutiveFails++;

      // In case of a lot of consecutive failures, send a heartbeat in order to
      // avoid timeout
      if (((consecutiveFails % heartbeatInterval) == 0) && (reporter != null)) {
        reporter.progress();
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return new String("FIL");
  }

  @Override
  public int getType() {
    return OperatorType.FILTER;
  }
}
