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

package org.apache.hadoop.hive.howl.data;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.schema.HSchema;

/**
 * Abstract class exposing get and set semantics for basic record usage.
 * Note :
 *   HowlRecord is designed only to be used as in-memory representation only.
 *   Don't use it to store data on the physical device.
 */
public abstract class HowlRecord implements HowlRecordable {

    public abstract Object get(String fieldName, HSchema recordSchema) throws HowlException;
    public abstract void set(String fieldName, HSchema recordSchema, Object value ) throws HowlException;

    
    protected Object get(String fieldName, HSchema recordSchema, Class clazz) throws HowlException{
        // TODO : if needed, verify that recordschema entry for fieldname matches appropriate type.
        return get(fieldName,recordSchema);
    }
    
    public Boolean getBoolean(String fieldName, HSchema recordSchema) throws HowlException {
        return (Boolean) get(fieldName, recordSchema, Boolean.class);
    }

    public void setBoolean(String fieldName, HSchema recordSchema, Boolean value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    public Byte getByte(String fieldName, HSchema recordSchema) throws HowlException {
        //TINYINT
        return (Byte) get(fieldName, recordSchema, Byte.class);
    }

    public void setByte(String fieldName, HSchema recordSchema, Byte value) throws HowlException {
        set(fieldName,recordSchema,value);
    }
    
    public Short getShort(String fieldName, HSchema recordSchema) throws HowlException {
        // SMALLINT
        return (Short) get(fieldName, recordSchema, Short.class);
    }

    public void setShort(String fieldName, HSchema recordSchema, Short value) throws HowlException {
        set(fieldName,recordSchema,value);
    }
    
    public Integer getInteger(String fieldName, HSchema recordSchema) throws HowlException {
        return (Integer) get(fieldName,recordSchema, Integer.class);
    }

    public void setInteger(String fieldName, HSchema recordSchema, Integer value) throws HowlException {
        set(fieldName,recordSchema,value);
    }
    
    public Long getLong(String fieldName, HSchema recordSchema) throws HowlException {
        // BIGINT
        return (Long) get(fieldName,recordSchema,Long.class);
    }

    public void setLong(String fieldName, HSchema recordSchema, Long value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    public Float getFloat(String fieldName, HSchema recordSchema) throws HowlException {
        return (Float) get(fieldName,recordSchema,Float.class);
    }

    public void setFloat(String fieldName, HSchema recordSchema, Float value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    public Double getDouble(String fieldName, HSchema recordSchema) throws HowlException {
        return (Double) get(fieldName,recordSchema,Double.class);
    }

    public void setDouble(String fieldName, HSchema recordSchema, Double value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    public String getString(String fieldName, HSchema recordSchema) throws HowlException {
        return (String) get(fieldName,recordSchema,String.class);
    }

    public void setString(String fieldName, HSchema recordSchema, String value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    @SuppressWarnings("unchecked")
    public List<? extends Object> getStruct(String fieldName, HSchema recordSchema) throws HowlException {
        return (List<? extends Object>) get(fieldName,recordSchema,List.class);
    }

    public void setStruct(String fieldName, HSchema recordSchema, List<? extends Object> value) throws HowlException {
        set(fieldName,recordSchema,value);
    }
    
    public List<?> getList(String fieldName, HSchema recordSchema) throws HowlException {
        return (List<?>) get(fieldName,recordSchema,List.class);
    }

    public void setList(String fieldName, HSchema recordSchema, List<?> value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

    public Map<?,?> getMap(String fieldName, HSchema recordSchema) throws HowlException {
        return (Map<?,?>) get(fieldName,recordSchema,Map.class);
    }
    
    public void setMap(String fieldName, HSchema recordSchema, Map<?,?> value) throws HowlException {
        set(fieldName,recordSchema,value);
    }

}
