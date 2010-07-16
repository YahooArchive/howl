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

import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * Class representing schema of a column in a table/partition
 */
public class HowlFieldSchema extends FieldSchema {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private HowlTypeInfo howlTypeInfo;
    
    
    /**
     * @param other
     */
    public HowlFieldSchema(FieldSchema other) {
        super(other);
        howlTypeInfo = new HowlTypeInfo(other.getType());
    }


    /**
     * @param name name of the field
     * @param type string representing type of the field
     * @param comment comment about the field
     */
    public HowlFieldSchema(String name, String type, String comment) {
        super(name, type, comment);
        howlTypeInfo = new HowlTypeInfo(type);
    }


    HowlTypeInfo getHowlTypeInfo() {
        return howlTypeInfo;
    }
}
