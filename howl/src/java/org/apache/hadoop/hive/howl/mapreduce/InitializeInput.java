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
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.data.HowlSchema;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;

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
//    System.err.println("XXX: all props:" + hiveConf.getAllProperties());
    if (inputInfo.getServerUri() != null){
      hiveConf.set("hive.metastore.local", "false");
      hiveConf.set("hive.metastore.uris", inputInfo.getServerUri());
    }

//    hiveConf.set("hive.metastore.warehouse.dir", conf.get("hive.metastore.warehouse.dir","/tmp/"));
//    hiveConf.setBoolean("hive.metastore.local", conf.getBoolean("hive.metastore.local",true));

    return (client = new HiveMetaStoreClient(hiveConf,null));
  }

  /**
   * Set the input to use for the Job. This queries the metadata server with the specified partition predicates,
   * gets the matching partitions, puts the information in the configuration object.
   * @param job the job object
   * @param inputInfo the howl table input info
   * @throws Exception
   */
  public static void setInput(Job job, HowlTableInfo inputInfo) throws Exception {

    //* Create and initialize an JobInfo object
    //* Serialize the JobInfo and save in the Job's Configuration object

    createHiveMetaClient(job.getConfiguration(),inputInfo);

    Table table = client.getTable(inputInfo.getDatabaseName(), inputInfo.getTableName());
    HowlSchema tableSchema = HowlUtil.getTableSchemaWithPtnCols(table);

    List<PartInfo> partInfoList = new ArrayList<PartInfo>();

    if( table.getPartitionKeys().size() != 0 ) {
      //Partitioned table
      List<Partition> parts = client.listPartitions(inputInfo.getDatabaseName(), inputInfo.getTableName(), MAX_PARTS);

      // populate partition info
      for (Partition ptn : parts){
        PartInfo partInfo = extractPartInfo(ptn.getSd(),ptn.getParameters());
        partInfo.setPartitionValues(ptn.getParameters());
        partInfoList.add(partInfo);
      }

    }else{
      //Non partitioned table
      PartInfo partInfo = extractPartInfo(table.getSd(),table.getParameters());
      partInfo.setPartitionValues(new HashMap<String,String>());
      partInfoList.add(partInfo);
    }

    JobInfo howlJobInfo = new JobInfo(inputInfo, tableSchema, partInfoList);
    inputInfo.setJobInfo(howlJobInfo);

    job.getConfiguration().set(
        HowlInputFormat.HOWL_KEY_JOB_INFO,
        HowlUtil.serialize(howlJobInfo)
    );
  }

  private static PartInfo extractPartInfo(StorageDescriptor sd, Map<String,String> parameters) throws IOException{
    HowlSchema schema = HowlUtil.extractSchemaFromStorageDescriptor(sd);
    String inputStorageDriverClass = null;
    Properties howlProperties = new Properties();
    if (parameters.containsKey(HOWL_ISD_CLASS)){
      inputStorageDriverClass = parameters.get(HOWL_ISD_CLASS);
    }else{
      throw new IOException("No input storage driver classname found, cannot read partition");
    }
    for (String key : parameters.keySet()){
      if (key.startsWith(HOWL_KEY_PREFIX)){
        howlProperties.put(key, parameters.get(key));
      }
    }
    return new PartInfo(schema,inputStorageDriverClass,  sd.getLocation(), howlProperties);
  }



  static StorerInfo extractStorerInfo(Map<String, String> properties) throws IOException {
    String inputSDClass, outputSDClass;

    if (properties.containsKey(HOWL_ISD_CLASS)){
      inputSDClass = properties.get(HOWL_ISD_CLASS);
    }else{
      throw new IOException("No input storage driver classname found for table, cannot write partition");
    }

    if (properties.containsKey(HOWL_OSD_CLASS)){
      outputSDClass = properties.get(HOWL_OSD_CLASS);
    }else{
      throw new IOException("No output storage driver classname found for table, cannot write partition");
    }

    Properties howlProperties = new Properties();
    for (String key : properties.keySet()){
      if (key.startsWith(HOWL_KEY_PREFIX)){
        howlProperties.put(key, properties.get(key));
      }
    }

    return new StorerInfo(inputSDClass, outputSDClass, howlProperties);
  }

}
