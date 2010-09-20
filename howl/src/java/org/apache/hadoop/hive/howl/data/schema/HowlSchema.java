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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.common.HowlException;

public class HowlSchema implements Serializable{
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private List<HowlFieldSchema> fieldSchemas;
    private Map<String,Integer> fieldPositionMap;
    private List<String> fieldNames = null;

    public HowlSchema(List<HowlFieldSchema> fieldSchemas){
        this.fieldSchemas = fieldSchemas;
        int idx = 0;
        fieldPositionMap = new HashMap<String,Integer>();
        fieldNames = new ArrayList<String>();
        for (HowlFieldSchema field : fieldSchemas){
            fieldPositionMap.put(field.getName(), idx);
            fieldNames.add(field.getName());
            idx++;
        }
    }

    public HowlSchema(HowlSchema other){
        this(other.getFields());
    }
    
    public List<HowlFieldSchema> getFields(){
        return this.fieldSchemas;
    }
    
    public Integer getPosition(String fieldName) throws HowlException {
        if (fieldPositionMap.containsKey(fieldName)){
            return fieldPositionMap.get(fieldName);
        }else{
            throw new HowlException("No field called "+fieldName+" found in schema argument");
        }
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
