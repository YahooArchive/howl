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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;

public class DefaultHowlRecord extends HowlRecord {

    private final List<Object> contents;

    public DefaultHowlRecord(){
        contents = new ArrayList<Object>();
    }

    public DefaultHowlRecord(int size){
        contents = new ArrayList<Object>(size);
        for(int i=0; i < size; i++){
            contents.set(i,null);
        }
    }

    @Override
    public void remove(int idx) throws HowlException {
      contents.remove(idx);
    }

    public DefaultHowlRecord(List<Object> list) {
        contents = list;
    }

    @Override
    public Object get(int fieldNum) {
        return contents.get(fieldNum);
    }

    @Override
    public List<Object> getAll() {
        return contents;
    }

    @Override
    public void set(int fieldNum, Object val) {
        contents.set(fieldNum, val);
    }

    @Override
    public int size() {
        return contents.size();
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        contents.clear();
        int len = in.readInt();
        for(int i =0; i < len; i++){
            contents.add(ReaderWriter.readDatum(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int sz = size();
        out.writeInt(sz);
        for (int i = 0; i < sz; i++) {
            ReaderWriter.writeDatum(out, contents.get(i));
        }

    }

    @Override
    public int compareTo(Object that) {

        if(that instanceof HowlRecord) {
            HowlRecord other = (HowlRecord)that;
            int mySz = this.size();
            int urSz = other.size();
            if(mySz != urSz) {
                return mySz - urSz;
            } else{
                for (int i = 0; i < mySz;i++) {
                    int c = DataType.compare(get(i), other.get(i));
                    if (c != 0) {
                        return c;
                    }
                }
            }
            return 0;
        } else {
            return DataType.compare(this, that);
        }
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (Object o : contents) {
            if (o != null) {
                hash = 31 * hash + o.hashCode();
            }
        }
        return hash;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        for(Object o : contents) {
            sb.append(o+"\t");
        }
        return sb.toString();
    }

    @Override
    public Object get(String fieldName, HowlSchema recordSchema) throws HowlException {
        return get(recordSchema.getPosition(fieldName));
    }

    @Override
    public void set(String fieldName, HowlSchema recordSchema, Object value) throws HowlException {
        set(recordSchema.getPosition(fieldName),value);
    }

}
