package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.MiniCluster;
import org.apache.hadoop.hive.howl.data.Pair;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class TestHowlLoader extends TestCase {

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String COMPLEX_TABLE = "junit_unparted_complex";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  MiniCluster cluster = MiniCluster.buildCluster();
  private Driver driver;

  String basicFile = "basic.input.data";
  String complexFile = "complex.input.data";

  Map<Integer,Pair<Integer,String>> basicInputData;
  Map<Integer,Tuple> complexInputData;

  private void dropTable(String tablename) throws IOException{
    driver.run("drop table "+tablename);
  }
  private void createTable(String tablename, String schema, String partitionedBy) throws IOException{
    String createTable;
    createTable = "create table "+tablename+"("+schema+") ";
    if ((partitionedBy != null)&&(!partitionedBy.trim().isEmpty())){
      createTable = createTable + "partitioned by ("+partitionedBy+") ";
    }
    createTable = createTable + "stored as RCFILE tblproperties('howl.isd'='org.apache.hadoop.hive.howl.drivers.RCFileInputStorageDriver'," +
        "'howl.osd'='org.apache.hadoop.hive.howl.drivers.RCFileOutputStorageDriver') ";
    int retCode = driver.run(createTable).getResponseCode();
    if(retCode != 0) {
      throw new IOException("Failed to create table. ["+createTable+"]");
    }
  }

  private void createTable(String tablename, String schema) throws IOException{
    createTable(tablename,schema,null);
  }

  @Override
  protected void setUp() throws Exception {

    // TODO: this really really needs to move to a setupBeforeClass, but that's not working

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    driver = new Driver(hiveConf);

    cleanup();

    createTable(BASIC_TABLE,"a int, b string");
//    createTable(COMPLEX_TABLE,
//        "name string, studentid int, "
//        + "contact struct<phno:string,email:string>, "
//        + "currently_registered_courses array<string>, "
//        + "current_grades map<string,string>, "
//        + "phnos array<struct<phno:string,type:string>");

    createTable(PARTITIONED_TABLE,"a int, b string","bkt int");


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

    MiniCluster.createInputFile(cluster, complexFile,
        new String[]{
        "Henry Jekyll\t42\t(415-253-6367,hjekyll@contemporary.edu.uk)\t{(PHARMACOLOGY),(PSYCHIATRY)},[PHARMACOLOGY#A-,PSYCHIATRY#B+],{(415-253-6367,cell),(408-253-6367,landline)}",
        "Edward Hyde\t1337\t(415-253-6367,anonymous@b44chan.org)\t{(CREATIVE_WRITING),(COPYRIGHT_LAW)},[CREATIVE_WRITING#A+,COPYRIGHT_LAW#D],{(415-253-6367,cell),(408-253-6367,landline)}",
        }
    );

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.setBatchOn();
    server.registerQuery("A = load '"+basicFile+"' as (a:int, b:chararray);");
    server.registerQuery("store A into '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.registerQuery("B = filter A by a < 2;");
    server.registerQuery("store B into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=0');");
    server.registerQuery("C = filter A by a >= 2;");
    server.registerQuery("store C into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=1');");
//    server.registerQuery("D = load '"+complexFile+"' as (name:string, studentid:int, contact:tuple(phno:string,email:string), currently_registered_courses:bag{innertup:tuple(course:string)}, current_grades:map[ ] , phnos :bag{innertup:tuple(phno:string,type:string)})");
//    server.registerQuery("store B into '"+COMPLEX_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.executeBatch();

  }
  private void cleanup() throws IOException {
    MiniCluster.deleteFile(cluster, basicFile);
    MiniCluster.deleteFile(cluster, complexFile);
    dropTable(BASIC_TABLE);
    dropTable(COMPLEX_TABLE);
    dropTable(PARTITIONED_TABLE);
  }

  @Override
  protected void tearDown() throws Exception {
    cleanup();
  }

  public void testSchemaLoadBasic() throws IOException{

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // test that schema was loaded correctly
    server.registerQuery("X = load '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Schema dumpedXSchema = server.dumpSchema("X");
    System.err.println("dumped schema : basic : "+dumpedXSchema.toString());
    List<FieldSchema> Xfields = dumpedXSchema.getFields();
    assertEquals(2,Xfields.size());
    assertTrue(Xfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Xfields.get(0).type == DataType.INTEGER);
    assertTrue(Xfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Xfields.get(1).type == DataType.CHARARRAY);

  }

  public void testReadDataBasic() throws IOException {
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    server.registerQuery("X = load '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Iterator<Tuple> XIter = server.openIterator("X");
    int numTuplesRead = 0;
    while( XIter.hasNext() ){
      Tuple t = XIter.next();
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      assertEquals(t.get(0),basicInputData.get(numTuplesRead).first);
      assertEquals(t.get(1),basicInputData.get(numTuplesRead).second);
      numTuplesRead++;
    }
    assertEquals(numTuplesRead,basicInputData.size());
  }

  public void disabled_testSchemaLoadComplex() throws IOException{

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // test that schema was loaded correctly
    server.registerQuery("K = load '"+COMPLEX_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Schema dumpedKSchema = server.dumpSchema("K");
    System.err.println("dumped schema : complex : "+dumpedKSchema.toString());
    List<FieldSchema> Kfields = dumpedKSchema.getFields();
    assertEquals(6,Kfields.size());

    assertTrue(Kfields.get(0).type == DataType.CHARARRAY);
    assertTrue(Kfields.get(0).alias.equalsIgnoreCase("name"));

    assertTrue(Kfields.get(1).type == DataType.INTEGER);
    assertTrue(Kfields.get(1).alias.equalsIgnoreCase("studentid"));

    assertTrue(Kfields.get(2).type == DataType.TUPLE);
    assertTrue(Kfields.get(2).alias.equalsIgnoreCase("contact"));
    {
      assertNotNull(Kfields.get(2).schema);
      assertTrue(Kfields.get(2).schema.getFields().size() == 2);
      assertTrue(Kfields.get(2).schema.getFields().get(0).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(0).alias.equalsIgnoreCase("phno"));
      assertTrue(Kfields.get(2).schema.getFields().get(1).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(1).alias.equalsIgnoreCase("email"));
    }
    assertTrue(Kfields.get(3).type == DataType.BAG);
    assertTrue(Kfields.get(3).alias.equalsIgnoreCase("currently_registered_courses"));
    {
      assertNotNull(Kfields.get(3).schema);
      assertTrue(Kfields.get(3).schema.getFields().size() == 1);
      assertTrue(Kfields.get(3).schema.getFields().get(0).type == DataType.TUPLE);
      assertNotNull(Kfields.get(3).schema.getFields().get(0).schema);
      assertTrue(Kfields.get(3).schema.getFields().get(0).schema.getFields().size() == 1);
      assertTrue(Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).alias.equalsIgnoreCase("course"));
    }
    assertTrue(Kfields.get(4).type == DataType.MAP);
    assertTrue(Kfields.get(4).alias.equalsIgnoreCase("current_grades"));
    assertTrue(Kfields.get(5).type == DataType.BAG);
    assertTrue(Kfields.get(5).alias.equalsIgnoreCase("phnos"));
    {
      assertNotNull(Kfields.get(5).schema);
      assertTrue(Kfields.get(5).schema.getFields().size() == 1);
      assertTrue(Kfields.get(5).schema.getFields().get(0).type == DataType.TUPLE);
      assertNotNull(Kfields.get(5).schema.getFields().get(0).schema);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().size() == 2);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).alias.equalsIgnoreCase("phno"));
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().get(1).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).alias.equalsIgnoreCase("type"));
    }

  }

  public void disabled_testReadPartitionedBasic() throws IOException {
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    server.registerQuery("W = load '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader() as (a,b,bkt);");
    Iterator<Tuple> WIter = server.openIterator("W");
    Collection<Pair<Integer,String>> valuesRead = new ArrayList<Pair<Integer,String>>();
    while( WIter.hasNext() ){
      Tuple t = WIter.next();
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      valuesRead.add(new Pair<Integer,String>((Integer)t.get(0),(String)t.get(1)));
    }
    assertEquals(basicInputData.size(),valuesRead.size());
    assertEquals(valuesRead,basicInputData.values());
  }

  public void disabled_testProjectionsBasic() throws IOException {

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // test that projections work - note, yes, this test currently fails.
    // But it does show what we hope we understand to be desired behaviour.
    // so while the test is disabled for now, we expect to resurrect it sometime

    server.registerQuery("Y = load '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader() as (a:int);");
    Schema dumpedYSchema = server.dumpSchema("Y");
    System.err.println("dumped schema Y : "+dumpedYSchema.toString());
    List<FieldSchema> Yfields = dumpedYSchema.getFields();
    assertEquals(1,Yfields.size());
    assertTrue(Yfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Yfields.get(0).type == DataType.INTEGER);

    int numTuplesRead = 0;
    Iterator<Tuple> YIter = server.openIterator("Y");
    while( YIter.hasNext() ){
      Tuple t = YIter.next();
      assertEquals(t.size(),1);
      assertTrue(t.get(0).getClass() == Integer.class);
      assertEquals(t.get(0),basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    assertEquals(numTuplesRead,basicInputData.size());
  }
}
