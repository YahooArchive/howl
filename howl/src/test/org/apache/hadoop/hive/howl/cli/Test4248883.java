package org.apache.hadoop.hive.howl.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

public class Test4248883 extends TestCase{

  private Driver howlDriver;

  @Override
  protected void setUp() throws Exception {

    HiveConf howlConf = new HiveConf(this.getClass());
    howlConf.set(ConfVars.PREEXECHOOKS.varname, "");
    howlConf.set(ConfVars.POSTEXECHOOKS.varname, "");
    howlConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    howlConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HowlSemanticAnalyzer.class.getName());
    howlDriver = new Driver(howlConf);
  }

  String query;
  private final String dbName = "test4248883_db";
  private final String tblName = "test4248883_tbl";

  public void testAlterTablePass() throws IOException{

    howlDriver.run("create database " + dbName);
    howlDriver.run("use " + dbName);
    howlDriver.run("create table " + tblName + " (a int) partitioned by (b string) stored as RCFILE");

    CommandProcessorResponse response;

    response = howlDriver.run("alter table " + tblName + " add partition (b='2') location '/tmp'");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());

    response = howlDriver.run("alter table " + tblName + " set fileformat INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT " +
        "'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'mydriver' outputdriver 'yourdriver'");
    assertEquals(0, response.getResponseCode());
    assertNull(response.getErrorMessage());

    howlDriver.run("drop table " + tblName);
    howlDriver.run("drop database " + dbName);
  }

}
