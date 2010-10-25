package org.apache.hadoop.hive.howl.pig.drivers;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.builtin.PigStorage;

public class HowlPigStorageInputDriver extends LoadFuncBasedInputDriver {

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {

    lf = new PigStorage();
    super.initialize(context, storageDriverArgs);
  }
}
