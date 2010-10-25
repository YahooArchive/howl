package org.apache.hadoop.hive.howl.pig;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.howl.pig.drivers.HowlPigStorageInputDriver;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class TestHowlPigStorageDriver extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testPigStorageDriver() throws IOException{

    HiveConf howlConf = new HiveConf(this.getClass());
    howlConf.set(ConfVars.PREEXECHOOKS.varname, "");
    howlConf.set(ConfVars.POSTEXECHOOKS.varname, "");
    howlConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    howlConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HowlSemanticAnalyzer.class.getName());

    String fsLoc = howlConf.get("fs.default.name");
    Path tblPath = new Path(fsLoc, "/test_pig/data");
    String anyExistingFileInCurDir = "ivy.xml";
    tblPath.getFileSystem(howlConf).copyFromLocalFile(new Path(anyExistingFileInCurDir),tblPath);

    Driver howlDriver = new Driver(howlConf);
    howlDriver.run("drop table junit_pigstorage");
    CommandProcessorResponse resp;
    String createTable = "create external table junit_pigstorage (a string) partitioned by (b string) stored as RCFILE";

    resp = howlDriver.run(createTable);
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage add partition (b='2010-10-10') location '"+new Path(fsLoc, "/test_pig")+"'");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage partition (b='2010-10-10') set fileformat inputformat '" + RCFileInputFormat.class.getName()
        +"' outputformat '"+RCFileOutputFormat.class.getName()+"' inputdriver '"+HowlPigStorageInputDriver.class.getName()+"' outputdriver 'non-existent'");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp =  howlDriver.run("desc extended junit_pigstorage partition (b='2010-10-10')");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    PigServer server = new PigServer(ExecType.LOCAL, howlConf.getAllProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery(" a = load 'junit_pigstorage' using "+HowlLoader.class.getName()+";");
    Iterator<Tuple> itr = server.openIterator("a");
    DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(anyExistingFileInCurDir))));
    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertTrue(t.get(0) instanceof String);
      assertTrue(t.get(1) instanceof String);
      assertEquals(stream.readLine(), t.get(0));
      assertEquals("2010-10-10", t.get(1));
    }
    assertEquals(0,stream.available());
    stream.close();
  }
}
