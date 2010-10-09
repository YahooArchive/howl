package org.apache.hadoop.hive.howl.common;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.hadoop.hive.metastore.HiveMetaStore;

public class HowlDaemon implements Daemon {

    private String[] args = null;

    public void init(DaemonContext context) throws Exception {
            args = context.getArguments();
    }

    public void start() throws Exception {
//            if(args == null) {
//                    return;
//            }
            HiveMetaStore.main(args);
    }

    public void stop() throws Exception {
            // System.exit is called after both
            // stop() and destroy() return. Trivial impl => we do nothing.
    }

    public void destroy() {
    }

}
