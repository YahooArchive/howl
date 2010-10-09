package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.util.Map;

import org.apache.hadoop.hive.howl.mapreduce.InitializeInput;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AddPartitionHook extends AbstractSemanticAnalyzerHook{

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    Map<String, String> tblProps;
    String tblName = ast.getChild(0).getText();
    try {
      tblProps = context.getHive().getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName).getParameters();
    } catch (HiveException he) {
      throw new SemanticException(he);
    }
    if(!(tblProps.containsKey(InitializeInput.HOWL_ISD_CLASS) && tblProps.containsKey(InitializeInput.HOWL_OSD_CLASS))){
      throw new SemanticException("Operation not supported. Partitions can be added only in a table created through Howl. It seems table "+tblName+" was not created through Howl.");
    }
    return ast;
  }
}
