package org.apache.hadoop.hive.howl.cli;

import java.io.FileNotFoundException;
import java.security.Permission;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.howl.common.HowlConstants;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

public class TestPermsGrp extends TestCase {

  private boolean isServerRunning = false;
  private static final String msPort = "20101";
  private HiveConf howlConf;
  private Warehouse clientWH;
  private Thread t;
  private HiveMetaStoreClient msc;

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      HiveMetaStore.main(new String[]{msPort});
    }

  }

  @Override
  protected void tearDown() throws Exception {
    System.setSecurityManager(securityManager);
  }

  @Override
  protected void setUp() throws Exception {

    if(isServerRunning) {
      return;
    }

    t = new Thread(new RunMS());
    t.start();
    Thread.sleep(10000);

    isServerRunning = true;

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());


    howlConf = new HiveConf(this.getClass());
    howlConf.set("hive.metastore.local", "false");
    howlConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);
    howlConf.setIntVar(HiveConf.ConfVars.METATORETHRIFTRETRIES, 3);

    howlConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HowlSemanticAnalyzer.class.getName());
    howlConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    howlConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    howlConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    clientWH = new Warehouse(howlConf);
    msc = new HiveMetaStoreClient(howlConf,null);

  }


  public void testCustomPerms() throws Exception {

    String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    String tblName = "simptbl";
    String typeName = "Person";

    try {

      // Lets first test for default permissions, this is the case when user specified nothing.
      Table tbl = getTable(dbName,tblName,typeName);
      msc.createTable(tbl);
      Path dfsPath = clientWH.getDefaultTablePath(dbName, tblName);
      assertTrue(dfsPath.getFileSystem(howlConf).getFileStatus(dfsPath).getPermission().equals(FsPermission.getDefault()));
      cleanupTbl(dbName, tblName, typeName);

      // Next user did specify perms.
      tbl = getTable(dbName,tblName,typeName);
      howlConf.set(HowlConstants.HOWL_PERMS, "drwxrwxrwx");
      msc.createTable(tbl);
      dfsPath = clientWH.getDefaultTablePath(dbName, tblName);
      assertTrue(dfsPath.getFileSystem(howlConf).getFileStatus(dfsPath).getPermission().equals(FsPermission.valueOf("drwxrwxrwx")));
      cleanupTbl(dbName, tblName, typeName);

      // User specified perms in invalid format.
      howlConf.set(HowlConstants.HOWL_PERMS, "rwx");
      // make sure create table fails.
      try{
        HowlCli.main(new String[]{"-e","create table simptbl (name string) stored as RCFILE", "-p","rwx"});
        assert false;
      }catch(Exception me){
        assertTrue(me instanceof ExitException);
      }
      // No physical dir gets created.
      dfsPath = clientWH.getDefaultTablePath(MetaStoreUtils.DEFAULT_DATABASE_NAME,tblName);
      try{
        dfsPath.getFileSystem(howlConf).getFileStatus(dfsPath);
        assert false;
      } catch(Exception fnfe){
        assertTrue(fnfe instanceof FileNotFoundException);
      }

      // And no metadata gets created.
      try{
        msc.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName);
        assert false;
      }catch (Exception e){
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("default.simptbl table not found", e.getMessage());
      }

      // test for invalid group name
      howlConf.set(HowlConstants.HOWL_PERMS, "drw-rw-rw-");
      howlConf.set(HowlConstants.HOWL_GROUP, "THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER");

      try{
        // create table must fail.
        HowlCli.main(new String[]{"-e","create table simptbl (name string) stored as RCFILE", "-p","rw-rw-rw-","-g","THIS_CANNOT_BE_A_VALID_GRP_NAME_EVER"});
        assert false;
      }catch (Exception me){
        assertTrue(me instanceof SecurityException);
      }

      try{
        // no metadata should get created.
        msc.getTable(dbName, tblName);
        assert false;
      }catch (Exception e){
        assertTrue(e instanceof NoSuchObjectException);
        assertEquals("default.simptbl table not found", e.getMessage());
      }

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testCustomPerms failed.");
      throw e;
    }
  }

  private void silentDropDatabase(String dbName) throws MetaException, TException {
    try {
      for (String tableName : msc.getTables(dbName, "*")) {
        msc.dropTable(dbName, tableName);
      }

    } catch (NoSuchObjectException e) {
    }
  }

  private void cleanupTbl(String dbName, String tblName, String typeName) throws NoSuchObjectException, MetaException, TException, InvalidOperationException{

    msc.dropTable(dbName, tblName);
    msc.dropType(typeName);
  }

  private Table getTable(String dbName, String tblName, String typeName) throws NoSuchObjectException, MetaException, TException, AlreadyExistsException, InvalidObjectException{

    msc.dropTable(dbName, tblName);
    silentDropDatabase(dbName);


    msc.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<FieldSchema>(1));
    typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    msc.createType(typ1);

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    sd.setCols(typ1.getFields());

    sd.setSerdeInfo(new SerDeInfo());
    return tbl;
  }

  protected static class ExitException extends SecurityException {
    private static final long serialVersionUID = -1982617086752946683L;
    public final int status;

    public ExitException(int status) {
      super("There is no escape!");
      this.status = status;
    }
  }

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  private SecurityManager securityManager;

}
