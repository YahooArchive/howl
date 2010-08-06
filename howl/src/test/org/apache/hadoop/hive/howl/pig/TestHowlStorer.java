package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.MiniCluster;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.util.UDFContext;

public class TestHowlStorer extends TestCase {

  MiniCluster cluster = MiniCluster.buildCluster();
  private Driver driver;

  @Override
  protected void setUp() throws Exception {

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    driver = new Driver(hiveConf);
  }

  String fileName = "input.data";

//  public void testStoreFuncMap() throws IOException{
//
//    driver.run("drop table junit_unparted");
//    String createTable = "create table junit_unparted(b string,arr_of_maps array<map<string,string>>) stored as RCFILE " +
//        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
//        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
//    int retCode = driver.run(createTable).getResponseCode();
//    if(retCode != 0) {
//      throw new IOException("Failed to create table.");
//    }
//
//    MiniCluster.deleteFile(cluster, fileName);
//    MiniCluster.createInputFile(cluster, fileName, new String[]{"test\t{([a#haddop,b#pig])}","data\t{([b#hive,a#howl])}"});
//
//    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//    UDFContext.getUDFContext().setClientSystemProps();
//    server.setBatchOn();
//    server.registerQuery("A = load '"+fileName+"' as (b:chararray,arr_of_maps:bag{mytup:tuple ( mymap:map[ ])});");
//    server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('','b:chararray,arr_of_maps:bag{mytup:tuple ( mymap:map[ ])}');");
//    server.executeBatch();
//
//
//
//    MiniCluster.deleteFile(cluster, fileName);
//
//    driver.run("select * from junit_unparted");
//    ArrayList<String> res = new ArrayList<String>();
//    driver.getResults(res);
//    driver.run("drop table junit_unparted");
//    Iterator<String> itr = res.iterator();
//    System.out.println(itr.next());
//    System.out.println(itr.next());
//   assertFalse(itr.hasNext());
//
//  }

  public void testStoreMultiTables() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE " +
        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    driver.run("drop table junit_unparted2");
    createTable = "create table junit_unparted2(a int, b string) stored as RCFILE " +
    "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
    "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a < 2;");
    server.registerQuery("store B into 'junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.registerQuery("C = filter A by a >= 2;");
    server.registerQuery("store C into 'junit_unparted2' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("select * from junit_unparted2");
    ArrayList<String> res2 = new ArrayList<String>();
    driver.getResults(res2);
    res.addAll(res2);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testStoreWithNoSchema() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE " +
        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('');");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testStoreWithNoCtorArgs() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE " +
        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());

  }

  public void testEmptyStore() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE " +
        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a > 100;");
    server.registerQuery("store B into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('','a:int,b:chararray');");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    assertFalse(itr.hasNext());

  }

  public void testBagNStruct() throws IOException{
  driver.run("drop table junit_unparted");
  String createTable = "create table junit_unparted(b string,a struct<a1:int>,  arr_of_struct array<string>, " +
  		"arr_of_struct2 array<struct<s1:string,s2:string>>,  arr_of_struct3 array<struct<s3:string>>) stored as RCFILE " +
      "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
      "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
  int retCode = driver.run(createTable).getResponseCode();
  if(retCode != 0) {
    throw new IOException("Failed to create table.");
  }

  MiniCluster.deleteFile(cluster, fileName);
  MiniCluster.createInputFile(cluster, fileName, new String[]{"zookeeper\t(2)\t{(pig)}\t{(pnuts,hdfs)}\t{(hadoop),(howl)}",
      "chubby\t(2)\t{(sawzall)}\t{(bigtable,gfs)}\t{(mapreduce),(howl)}"});

  PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
  UDFContext.getUDFContext().setClientSystemProps();
  server.setBatchOn();
  server.registerQuery("A = load '"+fileName+"' as (b:chararray, a:tuple(a1:int), arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)});");
  server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('','b:chararray, a:tuple(a1:int)," +
  		" arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)}');");
  server.executeBatch();



  MiniCluster.deleteFile(cluster, fileName);

  driver.run("select * from junit_unparted");
  ArrayList<String> res = new ArrayList<String>();
  driver.getResults(res);
  driver.run("drop table junit_unparted");
  Iterator<String> itr = res.iterator();
  assertEquals("zookeeper\t{\"a1\":2}\t[\"pig\"]\t[{\"s1\":\"pnuts\",\"s2\":\"hdfs\"}]\t[{\"s3\":\"hadoop\"},{\"s3\":\"howl\"}]", itr.next());
  assertEquals("chubby\t{\"a1\":2}\t[\"sawzall\"]\t[{\"s1\":\"bigtable\",\"s2\":\"gfs\"}]\t[{\"s3\":\"mapreduce\"},{\"s3\":\"howl\"}]",itr.next());
 assertFalse(itr.hasNext());

  }

  public void testStoreFuncAllSimpleTypes() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b float, c double, d bigint, e string) stored as RCFILE " +
        "tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      input[i] = i + "\t" + i * 2.1f +"\t"+ i*1.1d + "\t" + i * 2L +"\t"+"lets howl";
    }

    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:float, c:double, d:long, e:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('','a:int, b:float, c:double, d:long, e:chararray');");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);

    Iterator<String> itr = res.iterator();
    for(int i = 0; i < LOOP_SIZE*LOOP_SIZE; i++) {
      assertEquals( input[i] ,itr.next());
    }

    assertFalse(itr.hasNext());
    driver.run("drop table junit_unparted");
  }
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    MiniCluster.deleteFile(cluster, fileName);
  }




  public void testStoreFuncSimple() throws IOException{

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as RCFILE " +
    		"tblproperties('howl.isd'='org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver'," +
    		"'howl.osd'='org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    MiniCluster.deleteFile(cluster, fileName);
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE*LOOP_SIZE];
    int k = 0;
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        input[k++] = si + "\t"+j;
      }
    }
    MiniCluster.createInputFile(cluster, fileName, input);
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using org.apache.hadoop.hive.howl.pig.HowlStorer('','a:int,b:chararray');");
    server.executeBatch();
    MiniCluster.deleteFile(cluster, fileName);

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for(int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for(int j=1;j<=LOOP_SIZE;j++) {
        assertEquals( si + "\t"+j,itr.next());
      }
    }
   assertFalse(itr.hasNext());

  }


}
