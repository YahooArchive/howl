package org.apache.hadoop.hive.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RCFileMapReduceRecordReader<K extends LongWritable, V extends BytesRefArrayWritable>
  extends RecordReader<LongWritable,BytesRefArrayWritable>{

  private Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private LongWritable key;
  private BytesRefArrayWritable value;

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public BytesRefArrayWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    more = next(key);
    if (more) {
      in.getCurrentRow(value);
    }

    return more;
  }

  private boolean next(LongWritable key) throws IOException {
    if (!more) {
      return false;
    }

    more = in.next(key);
    if (!more) {
      return false;
    }

    if (in.lastSeenSyncPos() >= end) {
      more = false;
      return more;
    }
    return more;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {

    FileSplit fSplit = (FileSplit)split;
    Path path = fSplit.getPath();
    Configuration conf = context.getConfiguration();
    this.in = new RCFile.Reader(path.getFileSystem(conf), path, conf);
    this.end = fSplit.getStart() + fSplit.getLength();

    if(fSplit.getStart() > in.getPosition()) {
      in.sync(fSplit.getStart());
    }

    this.start = in.getPosition();
    more = start < end;

    key = new LongWritable();
    value = new BytesRefArrayWritable();
  }
}
