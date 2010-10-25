package org.apache.hadoop.hive.howl.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.howl.pig.drivers.HowlPigStorageInputDriver;
import org.apache.hadoop.mapreduce.JobContext;

public class MyPigStorageDriver extends HowlPigStorageInputDriver{

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {
    if ( !"control-A".equals(storageDriverArgs.getProperty(HowlPigStorageInputDriver.delim))){
      System.exit(1);
    }
    super.initialize(context, storageDriverArgs);
  }
}
