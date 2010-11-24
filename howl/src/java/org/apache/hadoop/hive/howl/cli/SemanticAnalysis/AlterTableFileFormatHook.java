package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.mapreduce.InitializeInput;
import org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver;
import org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AlterTableFileFormatHook extends AbstractSemanticAnalyzerHook {

  private String inDriver, outDriver, tableName;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {

    String inputFormat = null, outputFormat = null;
    tableName = BaseSemanticAnalyzer.unescapeIdentifier(((ASTNode)ast.getChild(0)).getChild(0).getText());
    ASTNode child =  (ASTNode)((ASTNode)ast.getChild(1)).getChild(0);

    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABLEFILEFORMAT:
      inputFormat  = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(0)).getToken().getText());
      outputFormat = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(1)).getToken().getText());
      inDriver     = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(2)).getToken().getText());
      outDriver    = BaseSemanticAnalyzer.unescapeSQLString(((ASTNode) child.getChild(3)).getToken().getText());
      break;

    case HiveParser.TOK_TBLSEQUENCEFILE:
      throw new SemanticException("Operation not supported. Howl doesn't support Sequence File by default yet. " +
      "You may specify it through INPUT/OUTPUT storage drivers.");

    case HiveParser.TOK_TBLTEXTFILE:
      throw new SemanticException("Operation not supported. Howl doesn't support Text File by default yet. " +
      "You may specify it through INPUT/OUTPUT storage drivers.");

    case HiveParser.TOK_TBLRCFILE:
      inputFormat = RCFileInputFormat.class.getName();
      outputFormat = RCFileOutputFormat.class.getName();
      inDriver = RCFileInputStorageDriver.class.getName();
      outDriver = RCFileOutputStorageDriver.class.getName();
      break;
    }

    if(inputFormat == null || outputFormat == null || inDriver == null || outDriver == null){
      throw new SemanticException("File format specification in command Alter Table file format is incorrect.");
    }
    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    Map<String, String> tblProps = new HashMap<String, String>(2);
    tblProps.put(InitializeInput.HOWL_ISD_CLASS, inDriver);
    tblProps.put(InitializeInput.HOWL_OSD_CLASS, outDriver);
    try {
      Hive db = context.getHive();
      Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      tbl.getTTable().getParameters().putAll(tblProps);
      db.alterTable(tableName, tbl);
    } catch (Exception he) {
      throw new SemanticException(he);
    }
  }
}
