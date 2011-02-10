package org.apache.hadoop.hive.howl.oozie;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;

public class JavaAction {

  public static void main(String[] args) throws Exception{

    HiveConf conf = new HiveConf();
    conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
    conf.setVar(ConfVars.SEMANTIC_ANALYZER_HOOK, HowlSemanticAnalyzer.class.getName());
    conf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    SessionState.start(new CliSessionState(conf));
    new CliDriver().processLine(args[0]);
  }

}
