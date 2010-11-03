package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

final class CreateDatabaseHook  extends AbstractSemanticAnalyzerHook{

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

    String databaseName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());

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

//  @Override
//  public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<? extends Serializable>> rootTasks) throws SemanticException {
//    // comparing to Table's postAnalyze, it doesn't seem like we need to do anything here.
//  }
}
