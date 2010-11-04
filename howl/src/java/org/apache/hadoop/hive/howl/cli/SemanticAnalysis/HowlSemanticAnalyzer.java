package org.apache.hadoop.hive.howl.cli.SemanticAnalysis;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.howl.common.AuthUtils;
import org.apache.hadoop.hive.howl.common.ErrorType;
import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HowlSemanticAnalyzer extends AbstractSemanticAnalyzerHook {

  private AbstractSemanticAnalyzerHook hook;
  private ASTNode ast;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    this.ast = ast;
    switch (ast.getToken().getType()) {

    // Howl wants to intercept following tokens and special-handle them.
    case HiveParser.TOK_CREATETABLE:
      hook = new CreateTableHook();
      return hook.preAnalyze(context, ast);

    // Howl will allow these operations to be performed since they are DDL statements.
    case HiveParser.TOK_DROPTABLE:
    case HiveParser.TOK_DESCTABLE:
    case HiveParser.TOK_ALTERTABLE_ADDCOLS:
    case HiveParser.TOK_ALTERTABLE_RENAME:
    case HiveParser.TOK_ALTERTABLE_DROPPARTS:
    case HiveParser.TOK_ALTERTABLE_PROPERTIES:
    case HiveParser.TOK_ALTERTABLE_SERIALIZER:
    case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
    case HiveParser.TOK_SHOW_TABLESTATUS:
    case HiveParser.TOK_SHOWTABLES:
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

    FsAction action;
    String tblName;

    try{

      switch (ast.getToken().getType()) {

      case HiveParser.TOK_DESCTABLE:
        tblName = getFullyQualifiedName((ASTNode) ast.getChild(0).getChild(0));
        action = FsAction.READ;
        break;

      case HiveParser.TOK_SHOWPARTITIONS:
        action = FsAction.READ;
        tblName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        break;

      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      case HiveParser.TOK_DROPTABLE:
      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      case HiveParser.TOK_ALTERTABLE_RENAME:
      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
      case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
        action = FsAction.WRITE;
        tblName = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
        break;

      case HiveParser.TOK_ALTERTABLE_PARTITION:
        action = FsAction.WRITE;
        tblName =  BaseSemanticAnalyzer.unescapeIdentifier(((ASTNode)ast.getChild(0)).getChild(0).getText());
        break;

      case HiveParser.TOK_SHOW_TABLESTATUS:
      case HiveParser.TOK_SHOWTABLES:
        // We do no checks for show tables. Its always allowed.
      case HiveParser.TOK_CREATETABLE:
        // No checks for Create Table, since its not possible to compute location
        // here easily. So, it is especially handled in CreateTable post hook.
        tblName = null;
        action = null;
        break;

      default:
        throw new HowlException(ErrorType.ERROR_INTERNAL_EXCEPTION, "Unexpected token: "+ast.getToken());
      }

      if(tblName != null){
        Path path;
        try {
          path = context.getHive().getTable(tblName).getPath();
          if(path != null){
            AuthUtils.authorize(new Warehouse(context.getConf()).getDnsPath(path), action, context.getConf());
          }
          else{
            // This will happen, if table exists in metastore for a given
            // tablename, but has no path associated with it, so there is nothing to check.
            // In such cases, do no checks and allow whatever hive behavior is for it.
          }
        } catch (MetaException e) {
          throw new SemanticException("Failed to compute location for: "+tblName,e);
        }
        catch (HiveException e) {
          throw new SemanticException("Failed to compute location for: "+tblName,e);
        }

      }
    }

    catch(HowlException e){
      throw new SemanticException(e);
    }

    if(hook != null){
      hook.postAnalyze(context, rootTasks);
    }
  }

  private String getFullyQualifiedName(ASTNode ast) {
    // Copied verbatim from DDLSemanticAnalyzer, since its private there.
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
  }
}
