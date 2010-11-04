package org.apache.hadoop.hive.howl.common;

import java.io.IOException;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

public class AuthUtils {

  /**
   * @param path non-null
   * @param action non-null
   * @param conf
   * @throws SemanticException
   * @throws HowlException
   *
   * This method validates only for existing path. If path doesn't exist
   * there is nothing to validate. So, make sure that path passed in is non-null.
   */

  @SuppressWarnings("deprecation")
  public static void authorize(final Path path, final FsAction action, final Configuration conf) throws SemanticException, HowlException{

    if(path == null) {
      throw new HowlException(ErrorType.ERROR_INTERNAL_EXCEPTION);
    }
    final FileStatus stat;

    try {
      stat = path.getFileSystem(conf).getFileStatus(path);
    } catch (AccessControlException ace) {
      throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL, ace);
    } catch (org.apache.hadoop.fs.permission.AccessControlException ace){
      // Older hadoop version will throw this @deprecated Exception.
      throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL, ace);
    } catch (IOException ioe){
      throw new SemanticException(ioe);
    }

    final UserGroupInformation ugi;
    try {
      ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    } catch (LoginException le) {
      throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL,le);
    } catch (IOException ioe) {
      throw new SemanticException(ioe);
    }

    final FsPermission dirPerms = stat.getPermission();

    final String user = ugi.getUserName();
    final String grp = stat.getGroup();
    if(user.equals(stat.getOwner())){
      if(dirPerms.getUserAction().implies(action)){
        return;
      }
      throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL);
    }
    if(ArrayUtils.contains(ugi.getGroupNames(), grp)){
      if(dirPerms.getGroupAction().implies(action)){
        return;
      }
      throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL);

    }
    if(dirPerms.getOtherAction().implies(action)){
      return;
    }
    throw new HowlException(ErrorType.ERROR_ACCESS_CONTROL);


  }
}
