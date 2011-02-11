package org.apache.howl.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.howl.pig.drivers.PigStorageInputDriver;

public class MyPigStorageDriver extends PigStorageInputDriver{

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {
    if ( !"control-A".equals(storageDriverArgs.getProperty(PigStorageInputDriver.delim))){
      /* This is the only way to make testcase fail. Throwing exception from
       * here doesn't propagate up.
       */
      System.exit(1);
    }
    super.initialize(context, storageDriverArgs);
  }
}
