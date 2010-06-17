package org.apache.hadoop.hive.howl.drivers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.HowlSchema;
import org.apache.hadoop.hive.howl.mapreduce.LoaderInfo;
import org.apache.hadoop.hive.io.RCFileMapReduceInputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class RCFileInputStorageDriver extends HowlInputStorageDriver{


  private final RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable> rcFileIF;
  private final SerDe serde;
  private static final Log LOG = LogFactory.getLog(RCFileInputStorageDriver.class);
  private final TupleFactory tupFac;
  private final Text text;
  private static final String HOWL_TBL_SCHEMA = "howl.tbl.schema";
  private static final String HOWL_PRJ_SCHEMA = "howl.prj.schema";

  public RCFileInputStorageDriver() throws SerDeException{

    rcFileIF = new RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable>();
    serde = new ColumnarSerDe();
    serde.initialize(new Configuration(), new Properties());
    tupFac = TupleFactory.getInstance();
    text = new Text();
  }

  @Override
  public InputFormat<?, ?> getInputFormat(LoaderInfo loaderInfo) {
    return rcFileIF;
  }

  @Override
  public void setInputPath(JobContext jobContext, String location) throws IOException {

    RCFileMapReduceInputFormat.setInputPaths(new Job(jobContext.getConfiguration()), location);
  }

  @Override
  public void setOriginalSchema(JobContext jobContext, HowlSchema howlSchema) throws IOException {

    jobContext.getConfiguration().set(HOWL_TBL_SCHEMA, howlSchema.getSchemaString());
  }

  @Override
  public void setOutputSchema(JobContext jobContext, HowlSchema howlSchema) throws IOException {

    jobContext.getConfiguration().set(HOWL_PRJ_SCHEMA, howlSchema.getSchemaString());

  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public BytesWritable convertKeyToBytesWritable(Object key) throws IOException {
    return super.convertKeyToBytesWritable(key);
  }

  @Override
  public Tuple convertValueToTuple(Object blob) throws IOException {

    BytesRefArrayWritable bytesRefArr = (BytesRefArrayWritable)blob;
    ColumnarStruct struct;
    try {
        struct = (ColumnarStruct)serde.deserialize(bytesRefArr);
    } catch (SerDeException e) {
        LOG.error(e.toString(), e);
        throw new IOException(e);
    }

    Tuple t = tupFac.newTuple();
    //read row fields
    List<Object> values = struct.getFieldsAsList(text);
    //for each value in the row convert to the correct pig type
    if(values != null && values.size() > 0){

        for(Object value : values){
            t.append(HiveRCSchemaUtil.extractPigTypeFromHiveType(value));
        }

    }

    return super.convertValueToTuple(value);
  }
}
