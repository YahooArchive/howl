package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.howl.ExitException;
import org.apache.hadoop.hive.howl.NoExitSecurityManager;
import org.apache.hadoop.hive.howl.cli.HowlCli;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TException;

public class TestPermsInheritance extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    msc = new HiveMetaStoreClient(conf);
    msc.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,"testNoPartTbl", true,true);
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
    msc.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,"testPartTbl", true,true);
    pig = new PigServer(ExecType.LOCAL, conf.getAllProperties());
    UDFContext.getUDFContext().setClientSystemProps();
  }

  private HiveMetaStoreClient msc;
  private SecurityManager securityManager;
  private PigServer pig;

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    System.setSecurityManager(securityManager);
  }

  private final HiveConf conf = new HiveConf(this.getClass());

  public void testNoPartTbl() throws IOException, MetaException, UnknownTableException, TException, NoSuchObjectException{

    try{
      HowlCli.main(new String[]{"-e","create table testNoPartTbl (line string) stored as RCFILE", "-p","rwx-wx---"});
    }
    catch(Exception e){
      assertTrue(e instanceof ExitException);
      assertEquals(((ExitException)e).getStatus(), 0);
    }
    Warehouse wh = new Warehouse(conf);
    Path dfsPath = wh.getDefaultTablePath(MetaStoreUtils.DEFAULT_DATABASE_NAME, "testNoPartTbl");
    FileSystem fs = dfsPath.getFileSystem(conf);
    assertEquals(fs.getFileStatus(dfsPath).getPermission(),FsPermission.valueOf("drwx-wx---"));

    pig.setBatchOn();
    pig.registerQuery("A  = load 'build.xml' as (line:chararray);");
    pig.registerQuery("store A into 'testNoPartTbl' using "+HowlStorer.class.getName()+"();");
    pig.executeBatch();
    FileStatus[] status = fs.listStatus(dfsPath,hiddenFileFilter);

    assertEquals(status.length, 1);
    assertEquals(FsPermission.valueOf("drwx-wx---"),status[0].getPermission());

    try{
      HowlCli.main(new String[]{"-e","create table testPartTbl (line string)  partitioned by (a string) stored as RCFILE", "-p","rwx-wx--x"});
    }
    catch(Exception e){
      assertTrue(e instanceof ExitException);
      assertEquals(((ExitException)e).getStatus(), 0);
    }

    dfsPath = wh.getDefaultTablePath(MetaStoreUtils.DEFAULT_DATABASE_NAME, "testPartTbl");
    assertEquals(fs.getFileStatus(dfsPath).getPermission(),FsPermission.valueOf("drwx-wx--x"));

    pig.setBatchOn();
    pig.registerQuery("A  = load 'build.xml' as (line:chararray);");
    pig.registerQuery("store A into 'testPartTbl' using "+HowlStorer.class.getName()+"('a=part');");
    pig.executeBatch();

    Path partPath = new Path(dfsPath,"a=part");
    assertEquals(FsPermission.valueOf("drwx-wx--x"),fs.getFileStatus(partPath).getPermission());
    status = fs.listStatus(partPath,hiddenFileFilter);
    assertEquals(status.length, 1);
    assertEquals(FsPermission.valueOf("drwx-wx--x"),status[0].getPermission());
  }

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
}
