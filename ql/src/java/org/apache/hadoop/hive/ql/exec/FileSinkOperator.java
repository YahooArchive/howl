/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.exec.FilterOperator.Counter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * File Sink operator implementation
 **/
public class FileSinkOperator extends TerminalOperator <fileSinkDesc> implements Serializable {

  public static interface RecordWriter {
    public void write(Writable w) throws IOException;
    public void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  transient protected RecordWriter outWriter;
  transient protected FileSystem fs;
  transient protected Path outPath;
  transient protected Path finalPath;
  transient protected Serializer serializer;
  transient protected BytesWritable commonKey = new BytesWritable();
  transient protected TableIdEnum tabIdEnum = null;
  transient private  LongWritable row_count;
  public static enum TableIdEnum {

    TABLE_ID_1_ROWCOUNT, TABLE_ID_2_ROWCOUNT, TABLE_ID_3_ROWCOUNT, TABLE_ID_4_ROWCOUNT, TABLE_ID_5_ROWCOUNT, TABLE_ID_6_ROWCOUNT, TABLE_ID_7_ROWCOUNT, TABLE_ID_8_ROWCOUNT, TABLE_ID_9_ROWCOUNT, TABLE_ID_10_ROWCOUNT, TABLE_ID_11_ROWCOUNT, TABLE_ID_12_ROWCOUNT, TABLE_ID_13_ROWCOUNT, TABLE_ID_14_ROWCOUNT, TABLE_ID_15_ROWCOUNT;

  }
  transient protected boolean autoDelete = false;

  private void commit() throws IOException {
    if(!fs.rename(outPath, finalPath)) {
      throw new IOException ("Unable to rename output to: " + finalPath);
    }
    LOG.info("Committed to output file: " + finalPath);
  }

  public void close(boolean abort) throws HiveException {
    if(!abort) {
      if (outWriter != null) {
        try {
          outWriter.close(abort);
          commit();
        } catch (IOException e) {
          // Don't throw an exception, just ignore and return
          return;
        }
      }
    } else {
      try {
        outWriter.close(abort);
        if(!autoDelete)
          fs.delete(outPath, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void initialize(Configuration hconf, Reporter reporter) throws HiveException {
    super.initialize(hconf, reporter);
    
    try {
      serializer = (Serializer)conf.getTableInfo().getDeserializerClass().newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());
      
  
      JobConf jc;
      if(hconf instanceof JobConf) {
        jc = (JobConf)hconf;
      } else {
        // test code path
        jc = new JobConf(hconf, ExecDriver.class);
      }

      int id = conf.getDestTableId();
      if ((id != 0) && (id <= TableIdEnum.values().length)){
        String enumName = "TABLE_ID_"+String.valueOf(id)+"_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);
        
      }
      fs = FileSystem.get(hconf);
      finalPath = new Path(Utilities.toTempPath(conf.getDirName()), Utilities.getTaskId(hconf));
      outPath = new Path(Utilities.toTempPath(conf.getDirName()), Utilities.toTempPath(Utilities.getTaskId(hconf)));

      LOG.info("Writing to temp file: " + outPath);

      HiveOutputFormat<?, ?> hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
      final Class<? extends Writable> outputClass = serializer.getSerializedClass();
      boolean isCompressed = conf.getCompressed();

      // The reason to keep these instead of using
      // OutputFormat.getRecordWriter() is that
      // getRecordWriter does not give us enough control over the file name that
      // we create.
      Path parent = Utilities.toTempPath(conf.getDirName());
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(parent, jc, hiveOutputFormat, isCompressed, finalPath);
      tableDesc tableInfo = conf.getTableInfo();

      this.outWriter = getRecordWriter(jc, hiveOutputFormat, outputClass, isCompressed, tableInfo.getProperties(), outPath);

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      try {
        Method deleteOnExit = FileSystem.class.getDeclaredMethod("deleteOnExit", new Class [] {Path.class});
        deleteOnExit.setAccessible(true);
        deleteOnExit.invoke(fs, outPath);
        autoDelete = true;
      } catch (Exception e) {}

    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public static RecordWriter getRecordWriter(JobConf jc, HiveOutputFormat<?, ?> hiveOutputFormat,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath) throws IOException, HiveException {
    if (hiveOutputFormat != null) {
      return hiveOutputFormat.getHiveRecordWriter(jc, outPath, valueClass, isCompressed, tableProp, null);
    }
    return null;
  }

  Writable recordValue; 
  public void process(Object row, ObjectInspector rowInspector, int tag) throws HiveException {
    try {
      if (reporter != null)
        reporter.progress();
      // user SerDe to serialize r, and write it out
      recordValue = serializer.serialize(row, rowInspector);
      if (row_count != null){
        row_count.set(row_count.get()+ 1);
      }
        
      outWriter.write(recordValue);
    } catch (IOException e) {
      throw new HiveException (e);
    } catch (SerDeException e) {
      throw new HiveException (e);
    }
  }

  /**
   * @return the name of the operator
   */
  public String getName() {
    return new String("FS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success) throws HiveException { 
    try {
      if(conf != null) {
        fs = FileSystem.get(hconf);
        Path tmpPath = Utilities.toTempPath(conf.getDirName());
        Path finalPath = new Path(conf.getDirName());
        if(success) {
          if(fs.exists(tmpPath)) {
            // Step1: rename tmp output folder to final path. After this point, 
            // updates from speculative tasks still writing to tmpPath will not 
            // appear in finalPath
            LOG.info("Moving tmp dir: " + tmpPath + " to: " + finalPath);
            renameOrMoveFiles(fs, tmpPath, finalPath);
            // Step2: Clean any temp files from finalPath
            Utilities.removeTempFiles(fs, finalPath);
          }
        } else {
          fs.delete(tmpPath);
        }
      }
    } catch (IOException e) {
      throw new HiveException (e);
    }
    super.jobClose(hconf, success);
  }
  
  /**
   * Rename src to dst, or in the case dst already exists, move files in src 
   * to dst.  If there is an existing file with the same name, the new file's 
   * name will be appended with "_1", "_2", etc.
   * @param fs the FileSystem where src and dst are on.  
   * @param src the src directory
   * @param dst the target directory
   * @throws IOException 
   */
  static public void renameOrMoveFiles(FileSystem fs, Path src, Path dst) throws IOException {
    if (!fs.exists(dst)) {
      fs.rename(src, dst);
    } else {
      // move file by file
      FileStatus[] files = fs.listStatus(src);
      for (int i=0; i<files.length; i++) {
        Path srcFilePath = files[i].getPath();
        String fileName = srcFilePath.getName();
        Path dstFilePath = new Path(dst, fileName);
        if (fs.exists(dstFilePath)) {
          int suffix = 0;
          do {
            suffix++;
            dstFilePath = new Path(dst, fileName + "_" + suffix);
          } while (fs.exists(dstFilePath));
        }
        fs.rename(srcFilePath, dstFilePath);
      }
    }
  }
}
