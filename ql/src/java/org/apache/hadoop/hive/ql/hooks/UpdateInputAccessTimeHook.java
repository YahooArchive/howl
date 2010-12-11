package org.apache.hadoop.hive.ql.hooks;

import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Implementation of a pre execute hook that updates the access
 * times for all the inputs.
 */
public class UpdateInputAccessTimeHook {

  private static final String LAST_ACCESS_TIME = "lastAccessTime";

  public static class PreExec implements PreExecute {
    Hive db;

    public void run(SessionState sess, Set<ReadEntity> inputs,
                    Set<WriteEntity> outputs, UserGroupInformation ugi)
      throws Exception {

      if (db == null) {
        try {
          db = Hive.get(sess.getConf());
        } catch (HiveException e) {
          // ignore
          db = null;
          return;
        }
      }

      int lastAccessTime = (int) (System.currentTimeMillis()/1000);

      for(ReadEntity re: inputs) {
        // Set the last query time
        ReadEntity.Type typ = re.getType();
        switch(typ) {
        // It is possible that read and write entities contain a old version
        // of the object, before it was modified by StatsTask.
        // Get the latest versions of the object
        case TABLE: {
          Table t = db.getTable(re.getTable().getTableName());
          t.setLastAccessTime(lastAccessTime);
          db.alterTable(t.getTableName(), t);
          break;
        }
        case PARTITION: {
          Partition p = re.getPartition();
          Table t = db.getTable(p.getTable().getTableName());
          p = db.getPartition(t, p.getSpec(), false);
          p.setLastAccessTime(lastAccessTime);
          db.alterPartition(t.getTableName(), p);
          t.setLastAccessTime(lastAccessTime);
          db.alterTable(t.getTableName(), t);
          break;
        }
        default:
          // ignore dummy inputs
          break;
        }
      }
    }
  }
}
