package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.mapreduce.InitializeInput;
import org.apache.hadoop.hive.howl.rcfile.RCFileInputDriver;
import org.apache.hadoop.hive.howl.rcfile.RCFileOutputDriver;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DDLWork;

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
      inDriver = RCFileInputDriver.class.getName();
      outDriver = RCFileOutputDriver.class.getName();
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

    Map<String,String> partSpec = ((DDLWork)rootTasks.get(rootTasks.size()-1).getWork()).getAlterTblDesc().getPartSpec();
    Map<String, String> howlProps = new HashMap<String, String>(2);
    howlProps.put(InitializeInput.HOWL_ISD_CLASS, inDriver);
    howlProps.put(InitializeInput.HOWL_OSD_CLASS, outDriver);

    try {
      Hive db = context.getHive();
      Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
      if(partSpec == null){
        // File format is for table; not for partition.
        tbl.getTTable().getParameters().putAll(howlProps);
        db.alterTable(tableName, tbl);
      }else{
        Partition part = db.getPartition(tbl,partSpec,false);
        Map<String,String> partParams = part.getParameters();
        if(partParams == null){
          partParams = new HashMap<String, String>();
        }
        partParams.putAll(howlProps);
        part.getTPartition().setParameters(partParams);
        db.alterPartition(tableName, part);
      }
    } catch (HiveException he) {
      throw new SemanticException(he);
    } catch (InvalidOperationException e) {
      throw new SemanticException(e);
    }
  }
}
