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

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDFToDate.
 *
 */
public class UDFToDate extends UDF {

  public UDFToDate() {
  }

  public java.sql.Date evaluate(String i) {
    if (i == null) {
      return null;
    } else {
      try {
        // Supported format: "YYYY-MM-DD"
        return java.sql.Date.valueOf(i);
      } catch (IllegalArgumentException e) {
        // We return NULL when the string is in a wrong format, which is
        // conservative.
        return null;
      }
    }
  }

  public java.sql.Date evaluate(Void i) {
    return null;
  }

  public java.sql.Date evaluate(Byte i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

  public java.sql.Date evaluate(Short i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

  public java.sql.Date evaluate(Integer i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

  public java.sql.Date evaluate(Long i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

  public java.sql.Date evaluate(Float i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

  public java.sql.Date evaluate(Double i) {
    if (i == null) {
      return null;
    } else {
      return new java.sql.Date(i.longValue());
    }
  }

}
