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
import org.apache.hadoop.io.Text;

public class UDFReverse extends UDF { 
  private Text result = new Text();
  
  public Text evaluate(Text s) {      
    if (s == null) {
      return null;
    }

    // Use a string because Text.getLength() returns the number of bytes.
    // This can be optimized by walking over the utf8 characters and not
    // creating a string at all.
    String text = s.toString();
    
    // Append the text to a StringBuffer in reverse order.
    StringBuffer revBuff = new StringBuffer();
    for (int i = text.length() - 1; i >= 0; i--) {
      revBuff.append(text.charAt(i));
    }
    
    result.set(revBuff.toString());
    return result;
  }
}
