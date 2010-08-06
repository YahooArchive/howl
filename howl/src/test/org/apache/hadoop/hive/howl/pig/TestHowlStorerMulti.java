package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.MiniCluster;
import org.apache.hadoop.hive.howl.data.Pair;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.util.UDFContext;

public class TestHowlStorerMulti extends TestCase {

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  private static MiniCluster cluster = MiniCluster.buildCluster();
  private static Driver driver;

  private static final String basicFile = "basic.input.data";

  private static Map<Integer,Pair<Integer,String>> basicInputData;

  private void dropTable(String tablename) throws IOException{
    driver.run("drop table "+tablename);
  }
  private void createTable(String tablename, String schema, String partitionedBy) throws IOException{
    String createTable;
    createTable = "create table "+tablename+"("+schema+") ";
    if ((partitionedBy != null)&&(!partitionedBy.trim().isEmpty())){
      createTable = createTable + "partitioned by ("+partitionedBy+") ";
    }
    createTable = createTable + "stored as RCFILE tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
    "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table. ["+createTable+"], return code from hive driver : ["+retCode+"]");
    }
  }

  private void createTable(String tablename, String schema) throws IOException{
    createTable(tablename,schema,null);
  }

  @Override
  protected void setUp() throws Exception {
    if (driver == null){
      HiveConf hiveConf = new HiveConf(this.getClass());
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      driver = new Driver(hiveConf);
    }
    cleanup();
  }

  @Override
  protected void tearDown() throws Exception {
    cleanup();
  }

  public void testStoreBasicTable() throws Exception {


    createTable(BASIC_TABLE,"a int, b string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+basicFile+"' as (a:int, b:chararray);");
    server.registerQuery("store A into '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");

    server.executeBatch();

    driver.run("select * from "+BASIC_TABLE);
    ArrayList<String> unpartitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(unpartitionedTableValuesReadFromHiveDriver);
    for(String line : unpartitionedTableValuesReadFromHiveDriver){
      System.err.println("basic : " + line);
    }

    assertEquals(basicInputData.size(),unpartitionedTableValuesReadFromHiveDriver.size());



  }

  public void testStorePartitionedTable() throws Exception {
    createTable(PARTITIONED_TABLE,"a int, b string","bkt string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+basicFile+"' as (a:int, b:chararray);");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=1');");

    server.executeBatch();

    driver.run("select * from "+PARTITIONED_TABLE);
    ArrayList<String> partitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(partitionedTableValuesReadFromHiveDriver);
    for(String line : partitionedTableValuesReadFromHiveDriver){
      System.err.println("ptned : " + line);
    }

    assertEquals(basicInputData.size(),partitionedTableValuesReadFromHiveDriver.size());

  }

  public void testStoreTableMulti() throws Exception {


    createTable(BASIC_TABLE,"a int, b string");
    createTable(PARTITIONED_TABLE,"a int, b string","bkt string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+basicFile+"' as (a:int, b:chararray);");
    server.registerQuery("store A into '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=1');");


    //    server.registerQuery("B = foreach A generate a,b;");
    //    server.registerQuery("B2 = filter B by a < 2;");
    //    server.registerQuery("store B2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=0');");
    //    server.registerQuery("C = foreach A generate a,b;");
    //    server.registerQuery("C2 = filter B by a >= 2;");
    //    server.registerQuery("store C2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=1');");

    server.executeBatch();

    driver.run("select * from "+BASIC_TABLE);
    ArrayList<String> unpartitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(unpartitionedTableValuesReadFromHiveDriver);
    for(String line : unpartitionedTableValuesReadFromHiveDriver){
      System.err.println("basic : " + line);
    }

    driver.run("select * from "+PARTITIONED_TABLE);
    ArrayList<String> partitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(partitionedTableValuesReadFromHiveDriver);
    for(String line : partitionedTableValuesReadFromHiveDriver){
      System.err.println("ptned : " + line);
    }

    assertEquals(basicInputData.size(),unpartitionedTableValuesReadFromHiveDriver.size());
    assertEquals(basicInputData.size(),partitionedTableValuesReadFromHiveDriver.size());

  }
  private void populateBasicFile() throws IOException {
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    basicInputData = new HashMap<Integer,Pair<Integer,String>>();
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        String sj = "S"+j+"S";
        input[k] = si + "\t" + sj;
        basicInputData.put(k, new Pair<Integer,String>(i,sj));
        k++;
      }
    }
    MiniCluster.createInputFile(cluster, basicFile, input);
  }
  private void cleanup() throws IOException {
    MiniCluster.deleteFile(cluster, basicFile);
    dropTable(BASIC_TABLE);
    dropTable(PARTITIONED_TABLE);
  }

}
