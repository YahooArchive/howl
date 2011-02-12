package org.apache.howl.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.howl.common.HowlConstants;

public class HowlDriver extends Driver {

  @Override
  public CommandProcessorResponse run(String command) {

    int ret = super.run(command).getResponseCode();

    SessionState ss = SessionState.get();

    if (ret == 0){
      // Only attempt to do this, if cmd was successful.
      ret = setFSPermsNGrp(ss);
    }
    // reset conf vars
    ss.getConf().set(HowlConstants.HOWL_CREATE_DB_NAME, "");
    ss.getConf().set(HowlConstants.HOWL_CREATE_TBL_NAME, "");

    return new CommandProcessorResponse(ret);
  }

  private int setFSPermsNGrp(SessionState ss) {

    Configuration conf =ss.getConf();

    String tblName = conf.get(HowlConstants.HOWL_CREATE_TBL_NAME,"");
    String dbName = conf.get(HowlConstants.HOWL_CREATE_DB_NAME, "");
    String grp = conf.get(HowlConstants.HOWL_GROUP,null);
    String permsStr = conf.get(HowlConstants.HOWL_PERMS,null);

    if(tblName.isEmpty() && dbName.isEmpty()){
      // it wasn't create db/table
      return 0;
    }

    if(null == grp && null == permsStr) {
      // there were no grp and perms to begin with.
      return 0;
    }

    FsPermission perms = FsPermission.valueOf(permsStr);
    if(!tblName.isEmpty()){
      Hive db = null;
      try{
        db = Hive.get();
        Table tbl =  db.getTable(tblName);
        Path tblPath = tbl.getPath();

        FileSystem fs = tblPath.getFileSystem(conf);
        if(null != perms){
          fs.setPermission(tblPath, perms);
        }
        if(null != grp){
          fs.setOwner(tblPath, null, grp);
        }
        return 0;

      } catch (Exception e){
          ss.err.println(String.format("Failed to set permissions/groups on TABLE: <%s> %s",tblName,e.getMessage()));
          try {  // We need to drop the table.
            if(null != db){ db.dropTable(tblName); }
          } catch (HiveException he) {
            ss.err.println(String.format("Failed to drop TABLE <%s> after failing to set permissions/groups on it. %s",tblName,e.getMessage()));
          }
          return 1;
      }
    }
    else{
      // looks like a db operation
      if (dbName.isEmpty() || dbName.equals(MetaStoreUtils.DEFAULT_DATABASE_NAME)){
        // We dont set perms or groups for default dir.
        return 0;
      }
      else{
        try{
          Path dbPath = new Warehouse(conf).getDefaultDatabasePath(dbName);
          FileSystem fs = dbPath.getFileSystem(conf);
          if(perms != null){
            fs.setPermission(dbPath, perms);
          }
          if(null != grp){
            fs.setOwner(dbPath, null, grp);
          }
          return 0;
        } catch (Exception e){
          ss.err.println(String.format("Failed to set permissions and/or group on DB: <%s> %s", dbName, e.getMessage()));
          try {
            Hive.get().dropDatabase(dbName);
          } catch (Exception e1) {
            ss.err.println(String.format("Failed to drop DB <%s> after failing to set permissions/group on it. %s", dbName, e1.getMessage()));
          }
          return 1;
        }
      }
    }
  }
}
