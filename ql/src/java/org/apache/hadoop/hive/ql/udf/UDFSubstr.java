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
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(
    name = "substr,substring",
    value = "_FUNC_(str, pos[, len]) - returns the substring of str that" +
    		" starts at pos and is of length len",
    extended = "pos is a 1-based index. If pos<0 the starting position is" +
    		" determined by counting backwards from the end of str.\n" +
    		"Example:\n " +
        "  > SELECT _FUNC_('Facebook', 5) FROM src LIMIT 1;\n" +
        "  'book'\n" +
        "  > SELECT _FUNC_('Facebook', -5) FROM src LIMIT 1;\n" +
        "  'ebook'\n" +
        "  > SELECT _FUNC_('Facebook', 5, 1) FROM src LIMIT 1;\n" +
        "  'b'"
    )
public class UDFSubstr extends UDF {

  Text r;
  public UDFSubstr() {
    r = new Text();
  }
  
  public Text evaluate(Text t, IntWritable pos, IntWritable len)  {
    
    if ((t == null) || (pos == null) || (len == null)) {
      return null;
    }
    
    r.clear();
    if ((len.get() <= 0)) {
      return r;
    }

    String s = t.toString();
    if ((Math.abs(pos.get()) > s.length())) {
      return r;
    }
    
    int start, end;

    if (pos.get() > 0) {
      start = pos.get() - 1;
    } else if (pos.get() < 0) {
      start = s.length() + pos.get();
    } else {
      start = 0;
    }
    
    if ((s.length() - start) < len.get()) {
      end = s.length();
    } else {
      end = start + len.get();
    }
    
    r.set(s.substring(start, end));
    return r;
  }

  IntWritable maxValue = new IntWritable(Integer.MAX_VALUE);
  
  public Text evaluate(Text s, IntWritable pos)  {
    return evaluate(s, pos, maxValue);
  }

}
