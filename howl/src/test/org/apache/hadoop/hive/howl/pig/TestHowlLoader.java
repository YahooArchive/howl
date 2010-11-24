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
  private static MiniCluster cluster = MiniCluster.buildCluster();
  private static Driver driver;

  private static final String basicFile = "basic.input.data";
  private static final String complexFile = "complex.input.data";

  private static int guardTestCount = 5; // ugh, instantiate using introspection in guardedSetupBeforeClass
  private static boolean setupHasRun = false;

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

  protected void guardedSetUpBeforeClass() throws Exception {
    if (!setupHasRun){
      setupHasRun = true;
      System.out.println("running setUp for the first time.");
    }else{
      System.out.println("setUp has already run previously, skipping.");
      return;
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    driver = new Driver(hiveConf);

    cleanup();

    createTable(BASIC_TABLE,"a int, b string");
    createTable(COMPLEX_TABLE,
        "name string, studentid int, "
        + "contact struct<phno:string,email:string>, "
        + "currently_registered_courses array<string>, "
        + "current_grades map<string,string>, "
        + "phnos array<struct<phno:string,type:string>>");

    createTable(PARTITIONED_TABLE,"a int, b string","bkt string");


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
//    Iterator<Tuple> iter = server.openIterator("A");
//    while(iter.hasNext()){
//      System.err.println("BOO A" + iter.next());
//    }
    server.registerQuery("store A into '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
//    server.registerQuery("A3 = load '"+ BASIC_TABLE +"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
//    iter = server.openIterator("A3");
//    while(iter.hasNext()){
//      System.err.println("BOO A3" + iter.next());
//    }
//
//    server.registerQuery("B = foreach A generate a,b;");
//    server.registerQuery("B2 = filter B by a < 2;");
//    server.registerQuery("store B2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=0');");
//    server.registerQuery("B3 = load '"+ PARTITIONED_TABLE +"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
//    iter = server.openIterator("B3");
//    while(iter.hasNext()){
//      System.err.println("BOO B3" + iter.next());
//    }
//
//    server.registerQuery("C = foreach A generate a,b;");
//    server.registerQuery("C2 = filter C by a >= 2;");
//    server.registerQuery("store C2 into '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer('bkt=1');");
//    server.registerQuery("C3 = load '"+ PARTITIONED_TABLE +"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
//    iter = server.openIterator("C3");
//    while(iter.hasNext()){
//      System.err.println("BOO C3" + iter.next());
//    }

    server.registerQuery("D = load '"+complexFile+"' as (name:chararray, studentid:int, contact:tuple(phno:chararray,email:chararray), currently_registered_courses:bag{innertup:tuple(course:chararray)}, current_grades:map[ ] , phnos :bag{innertup:tuple(phno:chararray,type:chararray)});");
    server.registerQuery("store D into '"+COMPLEX_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlStorer();");
    server.executeBatch();

  }
  private void cleanup() throws IOException {
    MiniCluster.deleteFile(cluster, basicFile);
    MiniCluster.deleteFile(cluster, complexFile);
    dropTable(BASIC_TABLE);
    dropTable(COMPLEX_TABLE);
    dropTable(PARTITIONED_TABLE);
  }

  protected void guardedTearDownAfterClass() throws Exception {
    guardTestCount--;
    if (guardTestCount > 0){
      System.out.println("skipping tearDown, "+guardTestCount+" more tests still need to run.");
      return;
    }
    System.out.println("running tearDown.");
    cleanup();
  }

  @Override
  protected void setUp() throws Exception {
    guardedSetUpBeforeClass();
  }

  @Override
  protected void tearDown() throws Exception {
    guardedTearDownAfterClass();
  }

  public void testSchemaLoadBasic() throws IOException{

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // test that schema was loaded correctly
    server.registerQuery("X = load '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Schema dumpedXSchema = server.dumpSchema("X");
//    System.err.println("dumped schema : basic : "+dumpedXSchema.toString());
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
      assertEquals(2,t.size());
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      assertEquals(t.get(0),basicInputData.get(numTuplesRead).first);
      assertEquals(t.get(1),basicInputData.get(numTuplesRead).second);
      numTuplesRead++;
    }
    assertEquals(basicInputData.size(),numTuplesRead);
  }

  public void testSchemaLoadComplex() throws IOException{

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // test that schema was loaded correctly
    server.registerQuery("K = load '"+COMPLEX_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Schema dumpedKSchema = server.dumpSchema("K");
//    System.err.println("dumped schema : complex : "+dumpedKSchema.toString());
    List<FieldSchema> Kfields = dumpedKSchema.getFields();
    assertEquals(6,Kfields.size());

    assertEquals(DataType.CHARARRAY,Kfields.get(0).type);
    assertEquals("name",Kfields.get(0).alias.toLowerCase());

    assertEquals( DataType.INTEGER,Kfields.get(1).type);
    assertEquals("studentid",Kfields.get(1).alias.toLowerCase());

    assertEquals(DataType.TUPLE,Kfields.get(2).type);
    assertEquals("contact",Kfields.get(2).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(2).schema);
      assertTrue(Kfields.get(2).schema.getFields().size() == 2);
      assertTrue(Kfields.get(2).schema.getFields().get(0).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(0).alias.equalsIgnoreCase("phno"));
      assertTrue(Kfields.get(2).schema.getFields().get(1).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(1).alias.equalsIgnoreCase("email"));
    }
    assertEquals(DataType.BAG,Kfields.get(3).type);
    assertEquals("currently_registered_courses",Kfields.get(3).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(3).schema);
      assertEquals(1,Kfields.get(3).schema.getFields().size());
      assertEquals(DataType.TUPLE,Kfields.get(3).schema.getFields().get(0).type);
      assertNotNull(Kfields.get(3).schema.getFields().get(0).schema);
      assertEquals(1,Kfields.get(3).schema.getFields().get(0).schema.getFields().size());
      assertEquals(DataType.CHARARRAY,Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).type);
      // assertEquals("course",Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).alias.toLowerCase());
      // commented out, because the name becomes "innerfield" by default - we call it "course" in pig,
      // but in the metadata, it'd be anonymous, so this would be autogenerated, which is fine
    }
    assertEquals(DataType.MAP,Kfields.get(4).type);
    assertEquals("current_grades",Kfields.get(4).alias.toLowerCase());
    assertEquals(DataType.BAG,Kfields.get(5).type);
    assertEquals("phnos",Kfields.get(5).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(5).schema);
      assertEquals(1,Kfields.get(5).schema.getFields().size());
      assertEquals(DataType.TUPLE,Kfields.get(5).schema.getFields().get(0).type);
      assertNotNull(Kfields.get(5).schema.getFields().get(0).schema);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().size() == 2);
      assertEquals(DataType.CHARARRAY,Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).type);
      assertEquals("phno",Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).alias.toLowerCase());
      assertEquals(DataType.CHARARRAY,Kfields.get(5).schema.getFields().get(0).schema.getFields().get(1).type);
      assertEquals("type",Kfields.get(5).schema.getFields().get(0).schema.getFields().get(1).alias.toLowerCase());
    }

  }

  public void testReadPartitionedBasic() throws IOException {
    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    driver.run("select * from "+PARTITIONED_TABLE);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    assertEquals(basicInputData.size(),valuesReadFromHiveDriver.size());

    server.registerQuery("W = load '"+PARTITIONED_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    Schema dumpedWSchema = server.dumpSchema("W");
    List<FieldSchema> Wfields = dumpedWSchema.getFields();
    assertEquals(3,Wfields.size());
    assertTrue(Wfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Wfields.get(0).type == DataType.INTEGER);
    assertTrue(Wfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Wfields.get(1).type == DataType.CHARARRAY);
    assertTrue(Wfields.get(2).alias.equalsIgnoreCase("bkt"));
    assertTrue(Wfields.get(2).type == DataType.CHARARRAY);

    Iterator<Tuple> WIter = server.openIterator("W");
    Collection<Pair<Integer,String>> valuesRead = new ArrayList<Pair<Integer,String>>();
    while( WIter.hasNext() ){
      Tuple t = WIter.next();
      assertTrue(t.size() == 3);
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      assertTrue(t.get(2).getClass() == String.class);
      valuesRead.add(new Pair<Integer,String>((Integer)t.get(0),(String)t.get(1)));
      if ((Integer)t.get(0) < 2){
        assertEquals("0",t.get(2));
      }else{
        assertEquals("1",t.get(2));
      }
    }
    assertEquals(valuesReadFromHiveDriver.size(),valuesRead.size());
  }

  public void testProjectionsBasic() throws IOException {

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

    // projections are handled by using generate, not "as" on the Load

    server.registerQuery("Y1 = load '"+BASIC_TABLE+"' using org.apache.hadoop.hive.howl.pig.HowlLoader();");
    server.registerQuery("Y2 = foreach Y1 generate a;");
    server.registerQuery("Y3 = foreach Y1 generate b,a;");
    Schema dumpedY2Schema = server.dumpSchema("Y2");
    Schema dumpedY3Schema = server.dumpSchema("Y3");
    List<FieldSchema> Y2fields = dumpedY2Schema.getFields();
    List<FieldSchema> Y3fields = dumpedY3Schema.getFields();
    assertEquals(1,Y2fields.size());
    assertEquals("a",Y2fields.get(0).alias.toLowerCase());
    assertEquals(DataType.INTEGER,Y2fields.get(0).type);
    assertEquals(2,Y3fields.size());
    assertEquals("b",Y3fields.get(0).alias.toLowerCase());
    assertEquals(DataType.CHARARRAY,Y3fields.get(0).type);
    assertEquals("a",Y3fields.get(1).alias.toLowerCase());
    assertEquals(DataType.INTEGER,Y3fields.get(1).type);

    int numTuplesRead = 0;
    Iterator<Tuple> Y2Iter = server.openIterator("Y2");
    while( Y2Iter.hasNext() ){
      Tuple t = Y2Iter.next();
      assertEquals(t.size(),1);
      assertTrue(t.get(0).getClass() == Integer.class);
      assertEquals(t.get(0),basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    numTuplesRead = 0;
    Iterator<Tuple> Y3Iter = server.openIterator("Y3");
    while( Y3Iter.hasNext() ){
      Tuple t = Y3Iter.next();
      assertEquals(t.size(),2);
      assertTrue(t.get(0).getClass() == String.class);
      assertEquals(t.get(0),basicInputData.get(numTuplesRead).second);
      assertTrue(t.get(1).getClass() == Integer.class);
      assertEquals(t.get(1),basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    assertEquals(basicInputData.size(),numTuplesRead);
  }
}
