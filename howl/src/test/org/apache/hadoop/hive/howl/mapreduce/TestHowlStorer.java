package org.apache.hadoop.hive.howl.mapreduce;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.howl.MiniCluster;
import org.apache.hadoop.hive.howl.Utils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.util.UDFContext;

public class TestHowlStorer extends TestCase {

  MiniCluster cluster = MiniCluster.buildCluster();
  @Override
  protected void setUp() throws Exception {
    super.setUp();

    MiniCluster.deleteFile(cluster, fileName);
      int LOOP_SIZE = 3;
      String[] input = new String[LOOP_SIZE*LOOP_SIZE];
      int k = 0;
      for(int i = 1; i <= LOOP_SIZE; i++) {
        String si = i + "";
        for(int j=1;j<=LOOP_SIZE;j++) {
          input[k++] = si + j;
        }
      }

      MiniCluster.createInputFile(cluster, fileName, input);

      Utils utils = new Utils();
      utils.createTestTable("default", "mytbl");

  }

  String fileName = "input.data";
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    MiniCluster.deleteFile(cluster, fileName);
    cluster.shutDown();
  }


  public void testStoreFunc() throws IOException{

    PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery("A = load '"+fileName+"' as (a:int, b:chararray);");
    server.registerQuery("B = foreach A generate $1;");
    server.registerQuery("store B into 'default.mytbl' using org.apache.hadoop.hive.howl.drivers.HowlStorer('','b:chararray');");
    server.executeBatch();
  }
}
