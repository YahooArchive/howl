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
package org.apache.hadoop.hive.howl.data.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.common.HowlException;

/**
 * HowlSchema. This class is NOT thread-safe.
 */

public class HowlSchema implements Serializable{

    private static final long serialVersionUID = 1L;

    private final List<HowlFieldSchema> fieldSchemas;
    private final Map<String,Integer> fieldPositionMap;
    private final List<String> fieldNames;

    /**
     *
     * @param fieldSchemas is now owned by HowlSchema. Any subsequent modifications
     * on fieldSchemas won't get reflected in HowlSchema.
     */
    public HowlSchema(final List<HowlFieldSchema> fieldSchemas){
        this.fieldSchemas = new ArrayList<HowlFieldSchema>(fieldSchemas);
        int idx = 0;
        fieldPositionMap = new HashMap<String,Integer>();
        fieldNames = new ArrayList<String>();
        for (HowlFieldSchema field : fieldSchemas){
            fieldPositionMap.put(field.getName(), idx);
            fieldNames.add(field.getName());
            idx++;
        }
    }

    public void append(final HowlFieldSchema hfs) throws HowlException{

      if(hfs == null || fieldSchemas == null){
        throw new HowlException("Attempt to append null HowlFieldSchema in HowlSchema.");
      }

      if(fieldNames.contains(hfs.fieldName))
        throw new HowlException("Attempt to append HowlFieldSchema with already existing name: " + hfs.fieldName + ".");

      this.fieldSchemas.add(hfs);
      String fieldName = hfs.getName();
      this.fieldNames.add(fieldName);
      this.fieldPositionMap.put(fieldName, this.size()-1);
    }

    /**
     *  Users are not allowed to modify the list directly, since HowlSchema
     *  maintains internal state. Use append/remove to modify the schema.
     */
    public List<HowlFieldSchema> getFields(){
        return Collections.unmodifiableList(this.fieldSchemas);
    }

    /**
     * @param fieldName
     * @return the index of field named fieldName in Schema. If field is not
     * present, returns null.
     */
    public Integer getPosition(String fieldName) {
      return fieldPositionMap.get(fieldName);
    }

    public HowlFieldSchema get(String fieldName) throws HowlException {
        return get(getPosition(fieldName));
    }

    public List<String> getFieldNames(){
        return this.fieldNames;
    }

    public HowlFieldSchema get(int position) {
        return fieldSchemas.get(position);
    }

    public int size(){
      return fieldSchemas.size();
    }

    public void remove(final HowlFieldSchema howlFieldSchema) throws HowlException {

      if(!fieldSchemas.contains(howlFieldSchema)){
        throw new HowlException("Attempt to delete a non-existent column from Howl Schema: "+ howlFieldSchema);
      }

      fieldSchemas.remove(howlFieldSchema);
      fieldPositionMap.remove(howlFieldSchema);
      fieldNames.remove(howlFieldSchema.getName());
    }

    @Override
    public String toString() {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (HowlFieldSchema hfs : fieldSchemas){
            if (!first){
                sb.append(",");
            }else{
                first = false;
            }
            if (hfs.getName() != null){
                sb.append(hfs.getName());
                sb.append(":");
            }
            sb.append(hfs.toString());
        }
        return sb.toString();
    }
}
