package org.apache.hadoop.hive.howl.pig.drivers;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.builtin.PigStorage;

public class PigStorageInputDriver extends LoadFuncBasedInputDriver {

  public static final String delim = "howl.pigstorage.delim";

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {

    lf = storageDriverArgs.containsKey(delim) ?
        new PigStorage(storageDriverArgs.getProperty(delim)) : new PigStorage();
    super.initialize(context, storageDriverArgs);
  }
}
