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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * The Class which handles querying the owl metadata server using the OwlDriver. The list of
 * partitions matching the partition filter is fetched from the server and the information is
 * serialized and written into the JobContext configuration. The inputInfo is also updated with
 * info required in the client process context.
 */
public class InitializeInput {

  /** The prefix for keys used for storage driver arguments */
  private static final String HOWL_KEY_PREFIX = "howl.";

  /** The key for the input storage driver class name */
  public static final String HOWL_ISD_CLASS = "howl.isd";

  /** The key for the output storage driver class name */
  public static final String HOWL_OSD_CLASS = "howl.osd";

  private static final short MAX_PARTS = Short.MAX_VALUE;

  static HiveMetaStoreClient client = null;

  private static HiveMetaStoreClient createHiveMetaClient(Configuration conf, HowlTableInfo inputInfo) throws Exception {
    if (client != null){
      return client;
    }
    HiveConf hiveConf = new HiveConf(HowlInputFormat.class);

    if (inputInfo.getServerUri() != null){
      hiveConf.set("hive.metastore.uris", inputInfo.getServerUri());
    } else if (! conf.get("hive.metastore.uris", "").equals("")){
      hiveConf.set("hive.metastore.uris", conf.get("hive.metastore.uris"));
    }
    hiveConf.set("hive.metastore.warehouse.dir", conf.get("hive.metastore.warehouse.dir","/tmp/"));
    hiveConf.setBoolean("hive.metastore.local", conf.getBoolean("hive.metastore.local",true));

    return (client = new HiveMetaStoreClient(hiveConf,null));
  }
  /**
   * Set the input to use for the Job. This queries the metadata server with the specified partition predicates,
   * gets the matching partitions, puts the information in the configuration object. The inputInfo object is
   * updated with information needed in the client process context.
   * @param job the job object
   * @param inputInfo the owl table input info
   * @throws Exception
   */
  public static void setInput(Job job, HowlTableInfo inputInfo) throws Exception {

    //* Create a OwlDriver instance with specified uri
    //* Call OwlDriver.getOwlTable to get the table schema
    //* Call OwlDriver.getPartitions to get list of partitions satisfying the given partition filter
    //* Create and initialize an OwlJobInfo object
    //* Save the OwlJobInfo object in the OwlTableInputInfo instance
    //* Serialize the OwlJobInfo and save in the Job's Configuration object

    createHiveMetaClient(job.getConfiguration(),inputInfo);

    Table table = client.getTable(inputInfo.getDatabaseName(), inputInfo.getTableName());
    Schema tableSchema = extractSchemaFromStorageDescriptor(table.getSd());

    List<Partition> parts = client.listPartitions(inputInfo.getDatabaseName(), inputInfo.getTableName(), MAX_PARTS);

    // convert List<OwlPartitionInfo> to List<OwlPartInfo>
    List<PartInfo> partInfoList = new ArrayList<PartInfo>();

    for (Partition ptn : parts){
      PartInfo partInfo = extractPartInfoFromStorageDescriptor(ptn.getSd());
      partInfo.setPartitionValues(ptn.getParameters());
      partInfoList.add(partInfo);
    }

    JobInfo howlJobInfo = new JobInfo(inputInfo, tableSchema, partInfoList);
    inputInfo.setJobInfo(howlJobInfo);

    job.getConfiguration().set(
        HowlInputFormat.HOWL_KEY_JOB_INFO,
        ObjectSerializer.serialize(howlJobInfo)
    );
  }

  private static PartInfo extractPartInfoFromStorageDescriptor(StorageDescriptor sd) throws IOException{
    if (sd == null){
      throw new IOException("Cannot construct partition info from an empty storage descriptor.");
    }
    Schema schema = extractSchemaFromStorageDescriptor(sd);
    String inputStorageDriverClass = null;
    Properties howlProperties = new Properties();
    if (sd.getParameters().containsKey(HOWL_ISD_CLASS)){
      inputStorageDriverClass = sd.getParameters().get(HOWL_ISD_CLASS);
    }else{
      throw new IOException("No input storage driver classname found, cannot read partition");
    }
    for (String key : sd.getParameters().keySet()){
      if (key.startsWith(HOWL_KEY_PREFIX)){
        howlProperties.put(key, sd.getParameters().get(key));
      }
    }
    return new PartInfo(schema,inputStorageDriverClass,  sd.getLocation(), howlProperties);
  }

  static Schema extractSchemaFromStorageDescriptor(StorageDescriptor sd) throws IOException {
    if (sd == null){
      throw new IOException("Cannot construct partition info from an empty storage descriptor.");
    }
    Schema schema = new Schema();
    schema.setFieldSchemas(sd.getCols());
    return schema;
  }


  static StorerInfo extractStorerInfo(StorageDescriptor sd) throws IOException {
    String inputSDClass, outputSDClass;
    Properties howlProperties = new Properties();


    if (sd.getParameters().containsKey(HOWL_ISD_CLASS)){
      inputSDClass = sd.getParameters().get(HOWL_ISD_CLASS);
    }else{
      throw new IOException("No input storage driver classname found for table, cannot write partition");
    }

    if (sd.getParameters().containsKey(HOWL_OSD_CLASS)){
      outputSDClass = sd.getParameters().get(HOWL_OSD_CLASS);
    }else{
      throw new IOException("No output storage driver classname found for table, cannot write partition");
    }

    for (String key : sd.getParameters().keySet()){
      if (key.startsWith(HOWL_KEY_PREFIX)){
        howlProperties.put(key, sd.getParameters().get(key));
      }
    }

    return new StorerInfo(inputSDClass, outputSDClass, howlProperties);
  }

}
