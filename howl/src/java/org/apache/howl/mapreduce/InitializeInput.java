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
package org.apache.howl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.howl.common.HowlConstants;
import org.apache.howl.common.HowlUtil;
import org.apache.howl.data.schema.HowlSchema;

/**
 * The Class which handles querying the metadata server using the MetaStoreClient. The list of
 * partitions matching the partition filter is fetched from the server and the information is
 * serialized and written into the JobContext configuration. The inputInfo is also updated with
 * info required in the client process context.
 */
public class InitializeInput {

  /** The prefix for keys used for storage driver arguments */
  private static final String HOWL_KEY_PREFIX = "howl.";

  private static HiveMetaStoreClient createHiveMetaClient(Configuration conf, HowlTableInfo inputInfo) throws Exception {
    HiveConf hiveConf = new HiveConf(HowlInputFormat.class);
    if (inputInfo.getServerUri() != null){
      hiveConf.setBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
      hiveConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
          inputInfo.getServerKerberosPrincipal());
      hiveConf.set("hive.metastore.local", "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, inputInfo.getServerUri());
    }

    return new HiveMetaStoreClient(hiveConf,null);
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

    HiveMetaStoreClient client = null;

    try {
      client = createHiveMetaClient(job.getConfiguration(),inputInfo);
      Table table = client.getTable(inputInfo.getDatabaseName(), inputInfo.getTableName());
      HowlSchema tableSchema = HowlUtil.getTableSchemaWithPtnCols(table);

      List<PartInfo> partInfoList = new ArrayList<PartInfo>();

      if( table.getPartitionKeys().size() != 0 ) {
        //Partitioned table
        List<Partition> parts = client.listPartitionsByFilter(
            inputInfo.getDatabaseName(), inputInfo.getTableName(),
            inputInfo.getFilter(), (short) -1);

        // populate partition info
        for (Partition ptn : parts){
          PartInfo partInfo = extractPartInfo(ptn.getSd(),ptn.getParameters());
          partInfo.setPartitionValues(createPtnKeyValueMap(table,ptn));
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
    } finally {
      if (client != null ) {
        client.close();
      }
    }
  }

  private static Map<String, String> createPtnKeyValueMap(Table table, Partition ptn) throws IOException{
    List<String> values = ptn.getValues();
    if( values.size() != table.getPartitionKeys().size() ) {
      throw new IOException("Partition values in partition inconsistent with table definition, table "
          + table.getTableName() + " has "
          + table.getPartitionKeys().size()
          + " partition keys, partition has " + values.size() + "partition values" );
    }

    Map<String,String> ptnKeyValues = new HashMap<String,String>();

    int i = 0;
    for(FieldSchema schema : table.getPartitionKeys()) {
      // CONCERN : the way this mapping goes, the order *needs* to be preserved for table.getPartitionKeys() and ptn.getValues()
      ptnKeyValues.put(schema.getName().toLowerCase(), values.get(i));
      i++;
    }

    return ptnKeyValues;
  }

  private static PartInfo extractPartInfo(StorageDescriptor sd, Map<String,String> parameters) throws IOException{
    HowlSchema schema = HowlUtil.extractSchemaFromStorageDescriptor(sd);
    String inputStorageDriverClass = null;
    Properties howlProperties = new Properties();
    if (parameters.containsKey(HowlConstants.HOWL_ISD_CLASS)){
      inputStorageDriverClass = parameters.get(HowlConstants.HOWL_ISD_CLASS);
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

    if (properties.containsKey(HowlConstants.HOWL_ISD_CLASS)){
      inputSDClass = properties.get(HowlConstants.HOWL_ISD_CLASS);
    }else{
      throw new IOException("No input storage driver classname found for table, cannot write partition");
    }

    if (properties.containsKey(HowlConstants.HOWL_OSD_CLASS)){
      outputSDClass = properties.get(HowlConstants.HOWL_OSD_CLASS);
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
