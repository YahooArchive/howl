package org.apache.hadoop.hive.howl.pig.drivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputStorageDriver;
import org.apache.hadoop.hive.howl.pig.PigHowlUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;


/**
 * This is a base class which wraps a Load func in HowlInputStorageDriver.
 * If you already have a LoadFunc, then this class along with LoadFuncBasedInputFormat
 * is doing all the heavy lifting. For a new Howl Input Storage Driver just extend it
 * and override the initialize(). {@link HowlPigStorageInputDriver} illustrates
 * that well.
 */
public abstract class LoadFuncBasedInputDriver extends HowlInputStorageDriver{

  private LoadFuncBasedInputFormat inputFormat;
  private HowlSchema dataSchema;
  private Map<String,String> partVals;
  private List<String> desiredColNames;
  protected LoadFunc lf;

  @Override
  public HowlRecord convertToHowlRecord(WritableComparable baseKey, Writable baseValue)
      throws IOException {

    List<Object> data = ((Tuple)baseValue).getAll();
    List<Object> howlRecord = new ArrayList<Object>(desiredColNames.size());

    /* Iterate through columns asked for in output schema, look them up in
     * original data schema. If found, put it. Else look up in partition columns
     * if found, put it. Else, its a new column, so need to put null. Map lookup
     * on partition map will return null, if column is not found.
     */
    for(String colName : desiredColNames){
      Integer idx = dataSchema.getPosition(colName);
      howlRecord.add( idx != null ? data.get(idx) : partVals.get(colName));
    }
    return new DefaultHowlRecord(howlRecord);
  }

  @Override
  public InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(
      Properties howlProperties) {

    return inputFormat;
  }

  @Override
  public void setOriginalSchema(JobContext jobContext, HowlSchema howlSchema) throws IOException {

    dataSchema = howlSchema;
  }

  @Override
  public void setOutputSchema(JobContext jobContext, HowlSchema howlSchema) throws IOException {

    desiredColNames = howlSchema.getFieldNames();
  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {

    partVals = partitionValues;
  }

  @Override
  public void initialize(JobContext context, Properties storageDriverArgs) throws IOException {

    lf.setLocation(location, new Job(context.getConfiguration()));
    inputFormat = new LoadFuncBasedInputFormat(lf, PigHowlUtil.getResourceSchema(dataSchema));
  }

  private String location;

  @Override
  public void setInputPath(JobContext jobContext, String location) throws IOException {

    this.location = location;
    super.setInputPath(jobContext, location);
  }
}
