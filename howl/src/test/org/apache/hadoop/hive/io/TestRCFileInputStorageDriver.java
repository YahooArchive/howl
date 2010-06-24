package org.apache.hadoop.hive.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.drivers.RCFileInputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputFormat.HowlOperation;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRCFileInputStorageDriver {

  private static Configuration conf = new Configuration();

  private static Path file;

  private static FileSystem fs;

  @BeforeClass
  public static void setup(){

    try {
      fs = FileSystem.getLocal(conf);
      Path dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test_rcfile");
      fs.delete(dir, true);
    } catch (Exception e) {
    }
  }


  @Test
  public void testConvertValueToTuple() throws IOException,InterruptedException{
    fs.delete(file, true);

    byte[][] record_1 = {"123".getBytes("UTF-8"), "456".getBytes("UTF-8"),
        "789".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
        "5.3".getBytes("UTF-8"), "howl and hadoop".getBytes("UTF-8"),
        new byte[0], "null".getBytes("UTF-8")};
    byte[][] record_2 = {"100".getBytes("UTF-8"), "200".getBytes("UTF-8"),
        "123".getBytes("UTF-8"), "1000".getBytes("UTF-8"),
        "5.3".getBytes("UTF-8"), "howl and hadoop".getBytes("UTF-8"),
        new byte[0], "null".getBytes("UTF-8")};

    RCFileOutputFormat.setColumnNumber(conf, 8);
    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null,
        new DefaultCodec());
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(record_1.length);
    for (int i = 0; i < record_1.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_1[i], 0,
          record_1[i].length);
      bytes.set(i, cu);
    }
    writer.append(bytes);
    BytesRefArrayWritable bytes2 = new BytesRefArrayWritable(record_2.length);
    for (int i = 0; i < record_2.length; i++) {
      BytesRefWritable cu = new BytesRefWritable(record_2[i], 0,
          record_2[i].length);
      bytes2.set(i, cu);
    }
    writer.append(bytes2);
    writer.close();
    BytesRefArrayWritable[] bytesArr = new BytesRefArrayWritable[]{bytes,bytes2};

    Schema schema = buildHiveSchema();
    RCFileInputStorageDriver sd = new RCFileInputStorageDriver();
    sd.isFeatureSupported(HowlOperation.PROJECTION_PUSHDOWN);
    JobContext jc = new JobContext(conf, new JobID());
    sd.setInputPath(jc, file.toString());
    InputFormat<?,?> iF = sd.getInputFormat(null);
    InputSplit split = iF.getSplits(jc).get(0);
    sd.setOriginalSchema(jc, schema);
    sd.setOutputSchema(jc, schema);
    sd.initialize(jc, null);

    TaskAttemptContext tac = new TaskAttemptContext(conf, new TaskAttemptID());
    RecordReader<?,?> rr = iF.createRecordReader(split,tac);
    rr.initialize(split, tac);
    HowlRecord[] tuples = getExpectedTuples();
    for(int j=0; j < 2; j++){
      Assert.assertTrue(rr.nextKeyValue());
      BytesRefArrayWritable w = (BytesRefArrayWritable)rr.getCurrentValue();
      Assert.assertEquals(bytesArr[j], w);
      HowlRecord t = sd.convertValueToHowlRecord(w);
      Assert.assertTrue(t.equals(tuples[j]));
      Assert.assertEquals(8, t.size());
      for(int i=0; i < 8; i++){
        Assert.assertEquals(t.get(i),tuples[j].get(i));
      }
    }
  }


  private HowlRecord[] getExpectedTuples(){

    List<Object> rec_1 = new ArrayList<Object>(8);
    rec_1.add(new Byte("123"));
    rec_1.add(new Short("456"));
    rec_1.add( new Integer(789));
    rec_1.add( new Long(1000L));
    rec_1.add( new Double(5.3D));
    rec_1.add( new String("howl and hadoop"));
    rec_1.add( null);
    rec_1.add( "null");

    HowlRecord tup_1 = new DefaultHowlRecord(rec_1);

    List<Object> rec_2 = new ArrayList<Object>(8);
    rec_2.add( new Byte("100"));
    rec_2.add( new Short("200"));
    rec_2.add( new Integer(123));
    rec_2.add( new Long(1000L));
    rec_2.add( new Double(5.3D));
    rec_2.add( new String("howl and hadoop"));
    rec_2.add( null);
    rec_2.add( "null");
    HowlRecord tup_2 = new DefaultHowlRecord(rec_2);

    return  new HowlRecord[]{tup_1,tup_2};

  }

  private Schema buildHiveSchema(){

    List<FieldSchema> fields = new ArrayList<FieldSchema>(8);
    fields.add(new FieldSchema("atinyint", "tinyint", ""));
    fields.add(new FieldSchema("asmallint", "smallint", ""));
    fields.add(new FieldSchema("aint", "int", ""));
    fields.add(new FieldSchema("along", "bigint", ""));
    fields.add(new FieldSchema("adouble", "double", ""));
    fields.add(new FieldSchema("astring", "string", ""));
    fields.add(new FieldSchema("anullint", "int", ""));
    fields.add(new FieldSchema("anullstring", "string", ""));

    Map<String,String> props = new HashMap<String, String>();
    props.put(Constants.SERIALIZATION_NULL_FORMAT, "null");
    props.put(Constants.SERIALIZATION_FORMAT, "9");
    return new Schema(fields,props);
  }
}
