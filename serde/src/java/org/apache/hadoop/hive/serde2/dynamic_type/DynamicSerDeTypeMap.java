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

package org.apache.hadoop.hive.serde2.dynamic_type;

import org.apache.thrift.TException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.*;
import org.apache.thrift.protocol.TType;

public class DynamicSerDeTypeMap extends DynamicSerDeTypeBase {

  public boolean isPrimitive() { return false; }
  public boolean isMap() { return true;}

  // production is: Map<FieldType(),FieldType()>

  private final byte FD_KEYTYPE = 0;
  private final byte FD_VALUETYPE = 1;

  // returns Map<?,?>
  public Class getRealType() {
    try {
      Class c = this.getKeyType().getRealType();
      Class c2 = this.getValueType().getRealType();
      Object o = c.newInstance();
      Object o2 = c2.newInstance();
      Map<?,?> l = Collections.singletonMap(o,o2);
      return l.getClass();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public DynamicSerDeTypeMap(int i) {
    super(i);
  }

  public DynamicSerDeTypeMap(thrift_grammar p, int i) {
    super(p,i);
  }

  public DynamicSerDeTypeBase getKeyType() {
    return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_KEYTYPE)).getMyType();
  }

  public DynamicSerDeTypeBase getValueType() {
    return (DynamicSerDeTypeBase)((DynamicSerDeFieldType)this.jjtGetChild(FD_VALUETYPE)).getMyType();
  }

  public String toString() {
    return "map<" + this.getKeyType().toString() + "," + this.getValueType().toString() + ">";
  }

  public Map<Object,Object> deserialize(Object reuse, TProtocol iprot)  throws SerDeException, TException, IllegalAccessException {
    HashMap<Object, Object> deserializeReuse;
    if (reuse != null) {
      deserializeReuse = (HashMap<Object, Object>)reuse;
      deserializeReuse.clear();
    } else {
      deserializeReuse = new HashMap<Object, Object>();
    }
    TMap themap = iprot.readMapBegin();
    if (themap == null) {
      return null;
    }
    // themap might be reused by the Protocol.
    int mapSize = themap.size;
    for(int i = 0; i < mapSize; i++) {
      Object key = this.getKeyType().deserialize(null, iprot);
      Object value = this.getValueType().deserialize(null, iprot);
      deserializeReuse.put(key,value);
    }

    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    iprot.readMapEnd();
    return deserializeReuse;
  }

  TMap serializeMap = null;
  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
  throws TException, SerDeException, NoSuchFieldException,
  IllegalAccessException {
    DynamicSerDeTypeBase keyType = this.getKeyType();
    DynamicSerDeTypeBase valueType = this.getValueType();

    org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol nullProtocol = 
      (oprot instanceof org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol)
      ? (org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol)oprot
      : null;
    
    assert(oi.getCategory() == ObjectInspector.Category.MAP);
    MapObjectInspector moi = (MapObjectInspector)oi;
    ObjectInspector koi = moi.getMapKeyObjectInspector();
    ObjectInspector voi = moi.getMapValueObjectInspector();

    Map<?,?> map = moi.getMap(o);
    serializeMap = new TMap(keyType.getType(), valueType.getType(), map.size());
    oprot.writeMapBegin(serializeMap);
    
    for(Iterator i = map.entrySet().iterator(); i.hasNext(); ) {
      Map.Entry it = (Map.Entry)i.next();
      Object key = it.getKey();
      Object value = it.getValue();
      keyType.serialize(key, koi, oprot);
      if (value == null) {
        assert(nullProtocol != null);
        nullProtocol.writeNull();
      } else {
        valueType.serialize(value, voi, oprot);
      }
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not get too crazy
    oprot.writeMapEnd();      
  }

  public byte getType() {
    return TType.MAP;
  }
};

