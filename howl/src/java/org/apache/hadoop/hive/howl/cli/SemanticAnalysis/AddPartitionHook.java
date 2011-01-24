package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.util.Map;

import org.apache.hadoop.hive.howl.common.HowlConstants;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AddPartitionHook extends AbstractSemanticAnalyzerHook{

  private String tblName, inDriver, outDriver;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    Map<String, String> tblProps;
    tblName = ast.getChild(0).getText();
    try {
      tblProps = context.getHive().getTable(tblName).getParameters();
    } catch (HiveException he) {
      throw new SemanticException(he);
    }

    inDriver = tblProps.get(HowlConstants.HOWL_ISD_CLASS);
    outDriver = tblProps.get(HowlConstants.HOWL_OSD_CLASS);

    if(inDriver == null  || outDriver == null){
      throw new SemanticException("Operation not supported. Partitions can be added only in a table created through Howl. It seems table "+tblName+" was not created through Howl.");
    }
    return ast;
  }

//  @Override
//  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
//      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
//
//    try {
//      Hive db = context.getHive();
//      Table tbl = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
//      for(Task<? extends Serializable> task : rootTasks){
//        System.err.println("PArt spec: "+((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec());
//        Partition part = db.getPartition(tbl,((DDLWork)task.getWork()).getAddPartitionDesc().getPartSpec(),false);
//        Map<String,String> partParams = part.getParameters();
//        if(partParams == null){
//          System.err.println("Part map null ");
//          partParams = new HashMap<String, String>();
//        }
//        partParams.put(InitializeInput.HOWL_ISD_CLASS, inDriver);
//        partParams.put(InitializeInput.HOWL_OSD_CLASS, outDriver);
//        part.getTPartition().setParameters(partParams);
//        db.alterPartition(tblName, part);
//      }
//    } catch (HiveException he) {
//      throw new SemanticException(he);
//    } catch (InvalidOperationException e) {
//      throw new SemanticException(e);
//    }
//  }
}



