package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.howl.mapreduce.InitializeInput;
import org.apache.hadoop.hive.howl.rcfile.RCFileInputStorageDriver;
import org.apache.hadoop.hive.howl.rcfile.RCFileOutputStorageDriver;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

final class CreateTableHook  extends AbstractSemanticAnalyzerHook{

  private String inStorageDriver, outStorageDriver;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
  throws SemanticException {

    Hive db;
    try {
      db = context.getHive();
    } catch (HiveException e) {
      throw new SemanticException("Couldn't get Hive DB instance in semantic analysis phase.", e);
    }

    // Analyze and create tbl properties object
    int numCh = ast.getChildCount();

    String inputFormat = null, outputFormat = null,
    tableName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);

      switch (child.getToken().getType()) {

      case HiveParser.TOK_QUERY: // CTAS
        throw new SemanticException("Operation not supported. Create table as Select is not a valid operation.");

      case HiveParser.TOK_TABLEBUCKETS:
        throw new SemanticException("Operation not supported. Howl doesn't allow Clustered By in create table.");

      case HiveParser.TOK_TBLSEQUENCEFILE:
        throw new SemanticException("Operation not supported. Howl doesn't support Sequence File by default yet. " +
        "You may specify it through INPUT/OUTPUT storage drivers.");

      case HiveParser.TOK_TBLTEXTFILE:
        throw new SemanticException("Operation not supported. Howl doesn't support Text File by default yet. " +
        "You may specify it through INPUT/OUTPUT storage drivers.");

      case HiveParser.TOK_LIKETABLE:

        String likeTableName;
        if (child.getChildCount() > 0 && (likeTableName = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText())) != null) {

          throw new SemanticException("Operation not supported. CREATE TABLE LIKE is not supported.");
//          Map<String, String> tblProps;
//          try {
//            tblProps = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, likeTableName).getParameters();
//          } catch (HiveException he) {
//            throw new SemanticException(he);
//          }
//          if(!(tblProps.containsKey(InitializeInput.HOWL_ISD_CLASS) && tblProps.containsKey(InitializeInput.HOWL_OSD_CLASS))){
//            throw new SemanticException("Operation not supported. Table "+likeTableName+" should have been created through Howl. Seems like its not.");
//          }
//          return ast;
        }
        break;

      case HiveParser.TOK_IFNOTEXISTS:
        try {
          List<String> tables = db.getTablesByPattern(tableName);
          if (tables != null && tables.size() > 0) { // table exists
            return null;
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        break;

      case HiveParser.TOK_TABLEPARTCOLS:
        List<FieldSchema> partCols = BaseSemanticAnalyzer.getColumns((ASTNode) child.getChild(0), false);
        for(FieldSchema fs : partCols){
          if(!fs.getType().equalsIgnoreCase("string")){
            throw new SemanticException("Operation not supported. Howl only supports partition columns of type string. " +
                "For column: "+fs.getName()+" Found type: "+fs.getType());
          }
        }
        break;

      case HiveParser.TOK_TABLEFILEFORMAT:
        if(child.getChildCount() < 4) {
          throw new SemanticException("Incomplete specification of File Format. You must provide InputFormat, OutputFormat, InputDriver, OutputDriver.");
        }
        inputFormat      = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(0).getText());
        outputFormat     = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(1).getText());
        inStorageDriver  = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(2).getText());
        outStorageDriver = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(3).getText());
        break;

      case HiveParser.TOK_TBLRCFILE:
        inputFormat      = RCFileInputFormat.class.getName();
        outputFormat     = RCFileOutputFormat.class.getName();
        inStorageDriver  = RCFileInputStorageDriver.class.getName();
        outStorageDriver = RCFileOutputStorageDriver.class.getName();
        break;

      }
    }

    if(inputFormat == null || outputFormat == null || inStorageDriver == null || outStorageDriver == null){
      throw new SemanticException("STORED AS specification is either incomplete or incorrect.");
    }

    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    if(rootTasks.size() == 0){
      // There will be no DDL task created in case if its CREATE TABLE IF NOT EXISTS
      return;
    }
    CreateTableDesc desc = ((DDLTask)rootTasks.get(rootTasks.size()-1)).getWork().getCreateTblDesc();
    if(desc == null){
      // Desc will be null if its CREATE TABLE LIKE
      return;
    }
    Map<String,String> tblProps = desc.getTblProps();
    if(tblProps == null) {
      // tblProps will be null if user didnt use tblprops in his CREATE TABLE cmd.
      tblProps = new HashMap<String, String>();
    }
    tblProps.put(InitializeInput.HOWL_ISD_CLASS, inStorageDriver);
    tblProps.put(InitializeInput.HOWL_OSD_CLASS, outStorageDriver);
    desc.setTblProps(tblProps);
  }
}
