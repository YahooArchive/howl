package org.apache.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.howl.common.HowlConstants;

final class CreateDatabaseHook  extends AbstractSemanticAnalyzerHook{

  String databaseName;

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

    databaseName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);

      switch (child.getToken().getType()) {

      case HiveParser.TOK_QUERY: // CTAS
        throw new SemanticException("Operation not supported. Create db as Select is not a valid operation.");

      case HiveParser.TOK_IFNOTEXISTS:
        try {
          List<String> dbs = db.getDatabasesByPattern(databaseName);
          if (dbs != null && dbs.size() > 0) { // db exists
            return null;
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        break;
      }
    }

    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    context.getConf().set(HowlConstants.HOWL_CREATE_DB_NAME, databaseName);
  }
}
