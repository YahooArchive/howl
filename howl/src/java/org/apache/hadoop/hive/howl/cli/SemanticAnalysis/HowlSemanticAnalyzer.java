package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HowlSemanticAnalyzer extends AbstractSemanticAnalyzerHook {

  private AbstractSemanticAnalyzerHook hook;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    switch (ast.getToken().getType()) {

    // Howl wants to intercept following tokens and special-handle them.
    case HiveParser.TOK_CREATETABLE:
      hook = new CreateTableHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_CREATEDATABASE:
      hook = new CreateDatabaseHook();
      return hook.preAnalyze(context, ast);

    // DML commands used in Howl where we use the same implementation as default Hive.
    case HiveParser.TOK_SHOWDATABASES:
    case HiveParser.TOK_DROPDATABASE:
    case HiveParser.TOK_SWITCHDATABASE:
      return ast;

    // Howl will allow these operations to be performed since they are DDL statements.
    case HiveParser.TOK_DROPTABLE:
    case HiveParser.TOK_DESCTABLE:
    case HiveParser.TOK_ALTERTABLE_ADDCOLS:
    case HiveParser.TOK_ALTERTABLE_RENAME:
    case HiveParser.TOK_ALTERTABLE_DROPPARTS:
    case HiveParser.TOK_ALTERTABLE_PROPERTIES:
    case HiveParser.TOK_ALTERTABLE_SERIALIZER:
    case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
    case HiveParser.TOK_SHOWTABLES:
    case HiveParser.TOK_SHOW_TABLESTATUS:
    case HiveParser.TOK_SHOWPARTITIONS:
      return ast;

    case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      hook = new AddPartitionHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_ALTERTABLE_PARTITION:
      if (((ASTNode)ast.getChild(1)).getToken().getType() == HiveParser.TOK_ALTERTABLE_FILEFORMAT) {
        hook = new AlterTableFileFormatHook();
        return hook.preAnalyze(context, ast);
      } else {
        return ast;
      }

    // In all other cases, throw an exception. Its a white-list of allowed operations.
    default:
      throw new SemanticException("Operation not supported.");

    }
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {

    if(hook != null){
      hook.postAnalyze(context, rootTasks);
    }
  }
}
