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
package org.apache.hadoop.hive.howl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HowlRecordWriter extends RecordWriter<WritableComparable<?>, HowlRecord> {

    private final HowlOutputStorageDriver storageDriver;
    private final RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter;

    public HowlRecordWriter(HowlOutputStorageDriver storageDriver,
        RecordWriter<? super WritableComparable<?>, ? super Writable> baseWriter) {
        this.storageDriver = storageDriver;
        this.baseWriter = baseWriter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
        baseWriter.close(context);
    }

    @Override
    public void write(WritableComparable<?> key, HowlRecord value) throws IOException,
            InterruptedException {
        //The key given by user is ignored
        WritableComparable<?> generatedKey = storageDriver.generateKey(value);
        Writable convertedValue = storageDriver.convertValue(value);
        baseWriter.write(generatedKey, convertedValue);
    }
}
