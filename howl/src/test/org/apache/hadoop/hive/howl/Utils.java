package org.apache.hadoop.hive.howl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.mapreduce.InitializeInput;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;

public class Utils {

  public void createTestTable(String dbName, String tblName) throws Exception{

    hiveConf = new HiveConf(getClass());

    try {
      client = new HiveMetaStoreClient(hiveConf,null);

      initTable( dbName, tblName);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private HiveMetaStoreClient client;
  private HiveConf hiveConf;

  private void initTable(String dbName, String tblName) throws Exception {

    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
    assert client.createDatabase(dbName, "howlTest_loc");

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("colname", Constants.STRING_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(fields);
    tbl.setSd(sd);

    //sd.setLocation("hdfs://tmp");
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(
        org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    tbl.setPartitionKeys(fields);

    Map<String, String> tableParams = new HashMap<String, String>();
    tableParams.put(InitializeInput.HOWL_OSD_CLASS, RCFileOutputFormat.class.getName());
    tableParams.put(InitializeInput.HOWL_ISD_CLASS, RCFileInputFormat.class.getName());
    tableParams.put("howl.testarg", "testArgValue");

    tbl.setParameters(tableParams);

    client.createTable(tbl);
  }
}
