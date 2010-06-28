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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.impl.util.ObjectSerializer;

class HowlOutputCommitter extends OutputCommitter {

    /** The underlying output committer */
    OutputCommitter baseCommitter;

    HowlOutputCommitter(OutputCommitter baseCommitter) {
        this.baseCommitter = baseCommitter;
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        baseCommitter.abortTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        baseCommitter.commitTask(context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return baseCommitter.needsTaskCommit(context);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        baseCommitter.setupJob(context);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        baseCommitter.setupTask(context);
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {
      if( baseCommitter != null ) { //TODO : remove after adding output storage driver
        baseCommitter.cleanupJob(context);
      }

      OutputJobInfo jobInfo = HowlOutputFormat.getJobInfo(context);
      try {
        HowlTableInfo tableInfo = jobInfo.getTableInfo();
        HiveMetaStoreClient client = HowlOutputFormat.createHiveClient(
            jobInfo.getTableInfo().getServerUri(), context.getConfiguration());

        Table table = client.getTable(tableInfo.getDatabaseName(), tableInfo.getTableName());
        StorerInfo storer = InitializeInput.extractStorerInfo(table.getSd());

        Partition partition = new Partition();
        partition.setDbName(tableInfo.getDatabaseName());
        partition.setTableName(tableInfo.getTableName());
        partition.setParameters(new HashMap<String, String>());
        partition.setSd(table.getSd());

        HowlOutputStorageDriver driver = HowlOutputFormat.getOutputDriverInstance(jobInfo.getStorerInfo());
        //default location set by metastore
        partition.getSd().setLocation(driver.getPartitionLocation(context,
                table.getSd().getLocation(), tableInfo.getPartitionValues()));

        List<String> values = new ArrayList<String>();
        for(FieldSchema schema : table.getPartitionKeys()) {
          values.add(tableInfo.getPartitionValues().get(schema.getName()));
        }
        partition.setValues(values);

        partition.getSd().getParameters().put(InitializeInput.HOWL_LOADER_INFO,
            ObjectSerializer.serialize(storer.getLoaderInfo()));

        client.add_partition(partition);
      } catch (Exception e) {
        throw new IOException("Error adding partition to metastore", e);
      }
    }
}
