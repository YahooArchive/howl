package org.apache.hadoop.hive.howl.oozie;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class JavaAction {

  public static void main(String[] args) throws Exception{

    HiveConf conf = new HiveConf();
    conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
    conf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HowlSemanticAnalyzer.class.getName());
    conf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    SessionState.start(new CliSessionState(conf));
    Driver driver = new Driver(conf);
    CommandProcessorResponse resp = driver.run(args[0]);
    String errMsg = resp.getErrorMessage();
    if(errMsg != null){
      System.out.println("Error message while executing DDL command: "+errMsg);
    }
    System.out.println("Response code while executing DDL command: "+resp.getResponseCode());
  }

}
