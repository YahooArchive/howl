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

package org.apache.hadoop.hive.ql.io;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.rmi.server.UID;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.LazyDecompressionCallback;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <code>RCFile</code>s, short of Record Columnar File, are flat files
 * consisting of binary key/value pairs, which shares much similarity with
 * <code>SequenceFile</code>.
 * 
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part. When writing, RCFile.Writer first holds records'
 * value bytes in memory, and determines a row split if the raw bytes size of
 * buffered records overflow a given parameter<tt>Writer.columnsBufferSize</tt>,
 * which can be set like: <code>conf.setInt(COLUMNS_BUFFER_SIZE_CONF_STR,
          4 * 1024 * 1024)</code> .
 * <p>
 * <code>RCFile</code> provides {@link Writer}, {@link Reader} and classes for
 * writing, reading respectively.
 * </p>
 * 
 * <p>
 * RCFile stores columns of a table in a record columnar way. It first
 * partitions rows horizontally into row splits. and then it vertically
 * partitions each row split in a columnar way. RCFile first stores the meta
 * data of a row split, as the key part of a record, and all the data of a row
 * split as the value part.
 * </p>
 * 
 * <p>
 * RCFile compresses values in a more fine-grained manner then record level
 * compression. However, It currently does not support compress the key part
 * yet. The actual compression algorithm used to compress key and/or values can
 * be specified by using the appropriate {@link CompressionCodec}.
 * </p>
 * 
 * <p>
 * The {@link Reader} is used to read and explain the bytes of RCFile.
 * </p>
 * 
 * <h4 id="Formats">RCFile Formats</h4>
 * 
 * 
 * <h5 id="Header">RC Header</h5>
 * <ul>
 * <li>version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of
 * actual version number (e.g. SEQ4 or SEQ6)</li>
 * <li>keyClassName -KeyBuffer's class name</li>
 * <li>valueClassName - ValueBuffer's class name</li>
 * <li>compression - A boolean which specifies if compression is turned on for
 * keys/values in this file.</li>
 * <li>blockCompression - always false. this field is kept for compatible with
 * SequeceFile's format</li>
 * <li>compression codec - <code>CompressionCodec</code> class which is used for
 * compression of keys and/or values (if compression is enabled).</li>
 * <li>metadata - {@link Metadata} for this file.</li>
 * <li>sync - A sync marker to denote end of the header.</li>
 * </ul>
 * 
 * <h5>RCFile Format</h5>
 * <ul>
 * <li><a href="#Header">Header</a></li>
 * <li>Record
 * <li>Key part
 * <ul>
 * <li>Record length in bytes</li>
 * <li>Key length in bytes</li>
 * <li>Number_of_rows_in_this_record(vint)</li>
 * <li>Column_1_ondisk_length(vint)</li>
 * <li>Column_1_row_1_value_plain_length</li>
 * <li>Column_1_row_2_value_plain_length</li>
 * <li>...</li>
 * <li>Column_2_ondisk_length(vint)</li>
 * <li>Column_2_row_1_value_plain_length</li>
 * <li>Column_2_row_2_value_plain_length</li>
 * <li>...</li>
 * </ul>
 * </li>
 * </li>
 * <li>Value part
 * <ul>
 * <li>Compressed or plain data of [column_1_row_1_value,
 * column_1_row_2_value,....]</li>
 * <li>Compressed or plain data of [column_2_row_1_value,
 * column_2_row_2_value,....]</li>
 * </ul>
 * </li>
 * </ul>
 * 
 */
public class RCFile {

  private static final Log LOG = LogFactory.getLog(RCFile.class);

  public static final String RECORD_INTERVAL_CONF_STR = "hive.io.rcfile.record.interval";

  public static final String COLUMN_NUMBER_METADATA_STR = "hive.io.rcfile.column.number";

  public static final String COLUMN_NUMBER_CONF_STR = "hive.io.rcfile.column.number.conf";

  /*
   * these header and Sync are kept from SequenceFile, for compatible of
   * SequenceFile's format.
   */
  private static final byte VERSION_WITH_METADATA = (byte) 6;
  private static final byte[] VERSION = new byte[] {
      (byte) 'S', (byte) 'E', (byte) 'Q', VERSION_WITH_METADATA
      };

  private static final int SYNC_ESCAPE = -1; // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16; // number of bytes in hash
  private static final int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points. */
  public static final int SYNC_INTERVAL = 100 * SYNC_SIZE;

  /**
   * KeyBuffer is the key of each record in RCFile. Its on-disk layout is as
   * below:
   * 
   * <ul>
   * <li>record length in bytes,it is the sum of bytes used to store the key
   * part and the value part.</li>
   * <li>Key length in bytes, it is how many bytes used by the key part.</li>
   * <li>number_of_rows_in_this_record(vint),</li>
   * <li>column_1_ondisk_length(vint),</li>
   * <li>column_1_row_1_value_plain_length,</li>
   * <li>column_1_row_2_value_plain_length,</li>
   * <li>....</li>
   * <li>column_2_ondisk_length(vint),</li>
   * <li>column_2_row_1_value_plain_length,</li>
   * <li>column_2_row_2_value_plain_length,</li>
   * <li>.... .</li>
   * <li>{the end of the key part}</li>
   * </ul>
   */
  static class KeyBuffer implements Writable {
    // each column's value length in a split
    private int[] eachColumnValueLen = null;
    private int[] eachColumnUncompressedValueLen = null;
    // stores each cell's length of a column in one DataOutputBuffer element
    private NonSyncDataOutputBuffer[] allCellValLenBuffer = null;
    // how many rows in this split
    private int numberRows = 0;
    // how many columns
    private int columnNumber = 0;

    KeyBuffer(int columnNumber) {
      this(0, columnNumber);
    }

    KeyBuffer(int numberRows, int columnNum) {
      columnNumber = columnNum;
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncDataOutputBuffer[columnNumber];
      this.numberRows = numberRows;
    }

    /**
     * add in a new column's meta data.
     * 
     * @param columnValueLen
     *          this total bytes number of this column's values in this split
     * @param colValLenBuffer
     *          each cell's length of this column's in this split
     */
    void setColumnLenInfo(int columnValueLen,
        NonSyncDataOutputBuffer colValLenBuffer,
        int columnUncompressedValueLen, int columnIndex) {
      eachColumnValueLen[columnIndex] = columnValueLen;
      eachColumnUncompressedValueLen[columnIndex] = columnUncompressedValueLen;
      allCellValLenBuffer[columnIndex] = colValLenBuffer;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      eachColumnValueLen = new int[columnNumber];
      eachColumnUncompressedValueLen = new int[columnNumber];
      allCellValLenBuffer = new NonSyncDataOutputBuffer[columnNumber];

      numberRows = WritableUtils.readVInt(in);
      for (int i = 0; i < columnNumber; i++) {
        eachColumnValueLen[i] = WritableUtils.readVInt(in);
        eachColumnUncompressedValueLen[i] = WritableUtils.readVInt(in);
        int bufLen = WritableUtils.readVInt(in);
        if (allCellValLenBuffer[i] == null) {
          allCellValLenBuffer[i] = new NonSyncDataOutputBuffer();
        } else {
          allCellValLenBuffer[i].reset();
        }
        allCellValLenBuffer[i].write(in, bufLen);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      // out.writeInt(numberRows);
      WritableUtils.writeVLong(out, numberRows);
      for (int i = 0; i < eachColumnValueLen.length; i++) {
        WritableUtils.writeVLong(out, eachColumnValueLen[i]);
        WritableUtils.writeVLong(out, eachColumnUncompressedValueLen[i]);
        NonSyncDataOutputBuffer colRowsLenBuf = allCellValLenBuffer[i];
        int bufLen = colRowsLenBuf.getLength();
        WritableUtils.writeVLong(out, bufLen);
        out.write(colRowsLenBuf.getData(), 0, bufLen);
      }
    }

    /**
     * get number of bytes to store the keyBuffer.
     * 
     * @return number of bytes used to store this KeyBuffer on disk
     * @throws IOException
     */
    public int getSize() throws IOException {
      int ret = 0;
      ret += WritableUtils.getVIntSize(numberRows);
      for (int i = 0; i < eachColumnValueLen.length; i++) {
        ret += WritableUtils.getVIntSize(eachColumnValueLen[i]);
        ret += WritableUtils.getVIntSize(eachColumnUncompressedValueLen[i]);
        ret += WritableUtils.getVIntSize(allCellValLenBuffer[i].getLength());
        ret += allCellValLenBuffer[i].getLength();
      }

      return ret;
    }
  }

  /**
   * ValueBuffer is the value of each record in RCFile. Its on-disk layout is as
   * below:
   * <ul>
   * <li>Compressed or plain data of [column_1_row_1_value,
   * column_1_row_2_value,....]</li>
   * <li>Compressed or plain data of [column_2_row_1_value,
   * column_2_row_2_value,....]</li>
   * </ul>
   */
  static class ValueBuffer implements Writable {

    class LazyDecompressionCallbackImpl implements LazyDecompressionCallback {

      int index = -1;
      int colIndex = -1;

      public LazyDecompressionCallbackImpl(int index, int colIndex) {
        super();
        this.index = index;
        this.colIndex = colIndex;
      }

      @Override
      public byte[] decompress() throws IOException {

        if (decompressedFlag[index] || codec == null) {
          return loadedColumnsValueBuffer[index].getData();
        }

        NonSyncDataOutputBuffer compressedData = loadedColumnsValueBuffer[index];
        NonSyncDataOutputBuffer decompressedData = new NonSyncDataOutputBuffer();
        decompressBuffer.reset();
        DataInputStream valueIn = new DataInputStream(deflatFilter);
        deflatFilter.resetState();
        decompressBuffer.reset(compressedData.getData(),
            keyBuffer.eachColumnValueLen[colIndex]);
        decompressedData.write(valueIn,
            keyBuffer.eachColumnUncompressedValueLen[colIndex]);
        loadedColumnsValueBuffer[index] = decompressedData;
        decompressedFlag[index] = true;
        return decompressedData.getData();
      }
    }

    // used to load columns' value into memory
    private NonSyncDataOutputBuffer[] loadedColumnsValueBuffer = null;
    private boolean[] decompressedFlag = null;
    private LazyDecompressionCallbackImpl[] lazyDecompressCallbackObjs = null;

    boolean inited = false;

    // used for readFields
    KeyBuffer keyBuffer;
    private int columnNumber = 0;

    // set true for columns that needed to skip loading into memory.
    boolean[] skippedColIDs = null;

    CompressionCodec codec;

    Decompressor valDecompressor = null;
    NonSyncDataInputBuffer decompressBuffer = new NonSyncDataInputBuffer();
    CompressionInputStream deflatFilter = null;

    public ValueBuffer(KeyBuffer keyBuffer) throws IOException {
      this(keyBuffer, null);
    }

    public ValueBuffer(KeyBuffer keyBuffer, boolean[] skippedColIDs)
        throws IOException {
      this(keyBuffer, keyBuffer.columnNumber, skippedColIDs, null);
    }

    public ValueBuffer(KeyBuffer currentKey, int columnNumber,
        boolean[] skippedCols, CompressionCodec codec) throws IOException {

      keyBuffer = currentKey;
      this.columnNumber = columnNumber;

      if (skippedCols != null && skippedCols.length > 0) {
        skippedColIDs = skippedCols;
      } else {
        skippedColIDs = new boolean[columnNumber];
        for (int i = 0; i < skippedColIDs.length; i++) {
          skippedColIDs[i] = false;
        }
      }

      int skipped = 0;
      if (skippedColIDs != null) {
        for (boolean currentSkip : skippedColIDs) {
          if (currentSkip) {
            skipped++;
          }
        }
      }
      loadedColumnsValueBuffer = new NonSyncDataOutputBuffer[columnNumber
          - skipped];
      decompressedFlag = new boolean[columnNumber - skipped];
      lazyDecompressCallbackObjs = new LazyDecompressionCallbackImpl[columnNumber
          - skipped];
      this.codec = codec;
      if (codec != null) {
        valDecompressor = CodecPool.getDecompressor(codec);
        deflatFilter = codec.createInputStream(decompressBuffer,
            valDecompressor);
      }

      for (int k = 0, readIndex = 0; k < columnNumber; k++) {
        if (skippedColIDs[k]) {
          continue;
        }
        loadedColumnsValueBuffer[readIndex] = new NonSyncDataOutputBuffer();
        if (codec != null) {
          decompressedFlag[readIndex] = false;
          lazyDecompressCallbackObjs[readIndex] = new LazyDecompressionCallbackImpl(
              readIndex, k);
        } else {
          decompressedFlag[readIndex] = true;
        }
        readIndex++;
      }
    }

    public void setColumnValueBuffer(NonSyncDataOutputBuffer valBuffer,
        int addIndex) {
      loadedColumnsValueBuffer[addIndex] = valBuffer;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int addIndex = 0;
      int skipTotal = 0;
      for (int i = 0; i < columnNumber; i++) {
        int vaRowsLen = keyBuffer.eachColumnValueLen[i];
        // skip this column
        if (skippedColIDs[i]) {
          skipTotal += vaRowsLen;
          continue;
        }

        if (skipTotal != 0) {
          in.skipBytes(skipTotal);
          skipTotal = 0;
        }

        NonSyncDataOutputBuffer valBuf = loadedColumnsValueBuffer[addIndex];
        valBuf.reset();
        valBuf.write(in, vaRowsLen);
        if (codec != null) {
          decompressedFlag[addIndex] = false;
        }
        addIndex++;
      }

      if (skipTotal != 0) {
        in.skipBytes(skipTotal);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      for (NonSyncDataOutputBuffer currentBuf : loadedColumnsValueBuffer) {
        out.write(currentBuf.getData(), 0, currentBuf.getLength());
      }
    }

    public void clearColumnBuffer() throws IOException {
      decompressBuffer.reset();
    }

    public void close() {
      for (NonSyncDataOutputBuffer element : loadedColumnsValueBuffer) {
        IOUtils.closeStream(element);
      }
      if (codec != null) {
        IOUtils.closeStream(decompressBuffer);
        CodecPool.returnDecompressor(valDecompressor);
      }
    }
  }

  /**
   * Write KeyBuffer/ValueBuffer pairs to a RCFile. RCFile's format is
   * compatible with SequenceFile's.
   * 
   */
  public static class Writer {

    Configuration conf;
    FSDataOutputStream out;

    CompressionCodec codec = null;
    Metadata metadata = null;
    Compressor compressor = null;

    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    long lastSyncPos; // position of last sync
    byte[] sync; // 16 random bytes
    {
      try {
        MessageDigest digester = MessageDigest.getInstance("MD5");
        long time = System.currentTimeMillis();
        digester.update((new UID() + "@" + time).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // how many records the writer buffers before it writes to disk
    private int RECORD_INTERVAL = Integer.MAX_VALUE;
    // the max size of memory for buffering records before writes them out
    private int columnsBufferSize = 4 * 1024 * 1024; // 4M
    // the conf string for COLUMNS_BUFFER_SIZE
    public static String COLUMNS_BUFFER_SIZE_CONF_STR = "hive.io.rcfile.record.buffer.size";

    // how many records already buffered
    private int bufferedRecords = 0;

    NonSyncDataOutputBuffer[] compressionBuffer;
    CompressionOutputStream[] deflateFilter = null;
    DataOutputStream[] deflateOut = null;
    private final ColumnBuffer[] columnBuffers;

    NonSyncDataOutputBuffer keyCompressionBuffer;
    CompressionOutputStream keyDeflateFilter;
    DataOutputStream keyDeflateOut;
    Compressor keyCompressor;

    private int columnNumber = 0;

    private final int[] columnValuePlainLength;

    KeyBuffer key = null;
    ValueBuffer value = null;

    /*
     * used for buffering appends before flush them out
     */
    class ColumnBuffer {
      // used for buffer a column's values
      NonSyncDataOutputBuffer columnValBuffer;
      // used to store each value's length
      NonSyncDataOutputBuffer valLenBuffer;

      /*
       * use a run-length encoding. We only record run length if a same
       * 'prevValueLen' occurs more than one time. And we negative the run
       * length to distinguish a runLength and a normal value length. For
       * example, if the values' lengths are 1,1,1,2, we record 1, ~2,2. And for
       * value lengths 1,2,3 we record 1,2,3.
       */
      int runLength = 0;
      int prevValueLength = -1;

      ColumnBuffer() throws IOException {
        columnValBuffer = new NonSyncDataOutputBuffer();
        valLenBuffer = new NonSyncDataOutputBuffer();
      }

      public void append(BytesRefWritable data) throws IOException {
        data.writeDataTo(columnValBuffer);
        int currentLen = data.getLength();

        if (prevValueLength < 0) {
          startNewGroup(currentLen);
          return;
        }

        if (currentLen != prevValueLength) {
          flushGroup();
          startNewGroup(currentLen);
        } else {
          runLength++;
        }
      }

      private void startNewGroup(int currentLen) {
        prevValueLength = currentLen;
        runLength = 0;
        return;
      }

      public void clear() throws IOException {
        valLenBuffer.reset();
        columnValBuffer.reset();
        prevValueLength = -1;
        runLength = 0;
      }

      public void flushGroup() throws IOException {
        if (prevValueLength >= 0) {
          WritableUtils.writeVLong(valLenBuffer, prevValueLength);
          if (runLength > 0) {
            WritableUtils.writeVLong(valLenBuffer, ~runLength);
          }
          runLength = -1;
          prevValueLength = -1;
        }
      }
    }

    public long getLength() throws IOException {
      return out.getPos();
    }

    /** Constructs a RCFile Writer. */
    public Writer(FileSystem fs, Configuration conf, Path name) throws IOException {
      this(fs, conf, name, null, new Metadata(), null);
    }

    /**
     * Constructs a RCFile Writer.
     * 
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
        Progressable progress, CompressionCodec codec) throws IOException {
      this(fs, conf, name, null, new Metadata(), codec);
    }

    /**
     * Constructs a RCFile Writer.
     * 
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @param progress
     * @param metadata
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name,
        Progressable progress, Metadata metadata, CompressionCodec codec) throws IOException {
      this(fs, conf, name, fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize(), progress,
          metadata, codec);
    }

    /**
     * 
     * Constructs a RCFile Writer.
     * 
     * @param fs
     *          the file system used
     * @param conf
     *          the configuration file
     * @param name
     *          the file name
     * @param bufferSize
     * @param replication
     * @param blockSize
     * @param progress
     * @param metadata
     * @throws IOException
     */
    public Writer(FileSystem fs, Configuration conf, Path name, int bufferSize,
        short replication, long blockSize, Progressable progress,
        Metadata metadata, CompressionCodec codec) throws IOException {
      RECORD_INTERVAL = conf.getInt(RECORD_INTERVAL_CONF_STR, RECORD_INTERVAL);
      columnNumber = conf.getInt(COLUMN_NUMBER_CONF_STR, 0);

      if (metadata == null) {
        metadata = new Metadata();
      }
      metadata.set(new Text(COLUMN_NUMBER_METADATA_STR), new Text(""
          + columnNumber));

      columnsBufferSize = conf.getInt(COLUMNS_BUFFER_SIZE_CONF_STR,
          4 * 1024 * 1024);

      columnValuePlainLength = new int[columnNumber];

      columnBuffers = new ColumnBuffer[columnNumber];
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i] = new ColumnBuffer();
      }

      init(name, conf, fs.create(name, true, bufferSize, replication,
          blockSize, progress), codec, metadata);
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
      key = new KeyBuffer(columnNumber);
      value = new ValueBuffer(key);
    }

    /** Write the initial part of file header. */
    void initializeFileHeader() throws IOException {
      out.write(VERSION);
    }

    /** Write the final part of file header. */
    void finalizeFileHeader() throws IOException {
      out.write(sync); // write the sync bytes
      out.flush(); // flush header
    }

    boolean isCompressed() {
      return codec != null;
    }

    /** Write and flush the file header. */
    void writeFileHeader() throws IOException {
      Text.writeString(out, KeyBuffer.class.getName());
      Text.writeString(out, ValueBuffer.class.getName());

      out.writeBoolean(isCompressed());
      out.writeBoolean(false);

      if (isCompressed()) {
        Text.writeString(out, (codec.getClass()).getName());
      }
      metadata.write(out);
    }

    void init(Path name, Configuration conf, FSDataOutputStream out,
        CompressionCodec codec, Metadata metadata) throws IOException {
      this.conf = conf;
      this.out = out;
      this.codec = codec;
      this.metadata = metadata;
      if (this.codec != null) {
        ReflectionUtils.setConf(codec, this.conf);
        compressor = CodecPool.getCompressor(codec);

        compressionBuffer = new NonSyncDataOutputBuffer[columnNumber];
        deflateFilter = new CompressionOutputStream[columnNumber];
        deflateOut = new DataOutputStream[columnNumber];
        for (int i = 0; i < columnNumber; i++) {
          compressionBuffer[i] = new NonSyncDataOutputBuffer();
          deflateFilter[i] = codec.createOutputStream(compressionBuffer[i],
              compressor);
          deflateOut[i] = new DataOutputStream(new BufferedOutputStream(
              deflateFilter[i]));
        }
        keyCompressor = CodecPool.getCompressor(codec);
        keyCompressionBuffer = new NonSyncDataOutputBuffer();
        keyDeflateFilter = codec.createOutputStream(keyCompressionBuffer,
            keyCompressor);
        keyDeflateOut = new DataOutputStream(new BufferedOutputStream(
            keyDeflateFilter));
      }
    }

    /** Returns the compression codec of data in this file. */
    public CompressionCodec getCompressionCodec() {
      return codec;
    }

    /** create a sync point. */
    public void sync() throws IOException {
      if (sync != null && lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE); // mark the start of the sync
        out.write(sync); // write sync
        lastSyncPos = out.getPos(); // update lastSyncPos
      }
    }

    /** Returns the configuration of this file. */
    Configuration getConf() {
      return conf;
    }

    private void checkAndWriteSync() throws IOException {
      if (sync != null && out.getPos() >= lastSyncPos + SYNC_INTERVAL) {
        sync();
      }
    }

    private int columnBufferSize = 0;

    /**
     * Append a row of values. Currently it only can accept <
     * {@link BytesRefArrayWritable}. If its <code>size()</code> is less than the
     * column number in the file, zero bytes are appended for the empty columns.
     * If its size() is greater then the column number in the file, the exceeded
     * columns' bytes are ignored.
     * 
     * @param val
     * @throws IOException
     */
    public void append(Writable val) throws IOException {

      if (!(val instanceof BytesRefArrayWritable)) {
        throw new UnsupportedOperationException(
            "Currently the writer can only accept BytesRefArrayWritable");
      }

      BytesRefArrayWritable columns = (BytesRefArrayWritable) val;
      int size = columns.size();
      for (int i = 0; i < size; i++) {
        BytesRefWritable cu = columns.get(i);
        int plainLen = cu.getLength();
        columnBufferSize += plainLen;
        columnValuePlainLength[i] += plainLen;
        columnBuffers[i].append(cu);
      }

      if (size < columnNumber) {
        for (int i = columns.size(); i < columnNumber; i++) {
          columnBuffers[i].append(BytesRefWritable.ZeroBytesRefWritable);
        }
      }

      bufferedRecords++;
      if ((columnBufferSize > columnsBufferSize)
          || (bufferedRecords >= RECORD_INTERVAL)) {
        flushRecords();
      }
    }

    private void flushRecords() throws IOException {

      key.numberRows = bufferedRecords;
      value.keyBuffer = key;

      int valueLength = 0;
      for (int columnIndex = 0; columnIndex < columnNumber; columnIndex++) {
        ColumnBuffer currentBuf = columnBuffers[columnIndex];
        currentBuf.flushGroup();

        NonSyncDataOutputBuffer columnValue = currentBuf.columnValBuffer;

        if (isCompressed()) {
          compressionBuffer[columnIndex].reset();
          deflateFilter[columnIndex].resetState();
          deflateOut[columnIndex].write(columnValue.getData(), 0, columnValue
              .getLength());
          deflateOut[columnIndex].flush();
          deflateFilter[columnIndex].finish();
          int colLen = compressionBuffer[columnIndex].getLength();
          key.setColumnLenInfo(colLen, currentBuf.valLenBuffer,
              columnValuePlainLength[columnIndex], columnIndex);
          value.setColumnValueBuffer(compressionBuffer[columnIndex],
              columnIndex);
          valueLength += colLen;
        } else {
          int colLen = columnValuePlainLength[columnIndex];
          key.setColumnLenInfo(colLen, currentBuf.valLenBuffer, colLen,
              columnIndex);
          value.setColumnValueBuffer(columnValue, columnIndex);
          valueLength += colLen;
        }
        columnValuePlainLength[columnIndex] = 0;
      }

      int keyLength = key.getSize();
      if (keyLength < 0) {
        throw new IOException("negative length keys not allowed: " + key);
      }

      // Write the record out
      checkAndWriteSync(); // sync
      out.writeInt(keyLength + valueLength); // total record length
      out.writeInt(keyLength); // key portion length
      if (!isCompressed()) {
        out.writeInt(keyLength);
        key.write(out); // key
      } else {
        keyCompressionBuffer.reset();
        keyDeflateFilter.resetState();
        key.write(keyDeflateOut);
        keyDeflateOut.flush();
        keyDeflateFilter.finish();
        int compressedKeyLen = keyCompressionBuffer.getLength();
        out.writeInt(compressedKeyLen);
        out.write(keyCompressionBuffer.getData(), 0, compressedKeyLen);
      }
      value.write(out); // value

      // clear the columnBuffers
      clearColumnBuffers();

      bufferedRecords = 0;
      columnBufferSize = 0;
    }

    private void clearColumnBuffers() throws IOException {
      for (int i = 0; i < columnNumber; i++) {
        columnBuffers[i].clear();
      }
    }

    public synchronized void close() throws IOException {
      if (bufferedRecords > 0) {
        flushRecords();
      }
      clearColumnBuffers();

      if (isCompressed()) {
        for (int i = 0; i < columnNumber; i++) {
          deflateFilter[i].close();
          IOUtils.closeStream(deflateOut[i]);
        }
        keyDeflateFilter.close();
        IOUtils.closeStream(keyDeflateOut);
        CodecPool.returnCompressor(keyCompressor);
        keyCompressor = null;
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      if (out != null) {

        // Close the underlying stream if we own it...
        out.flush();
        out.close();
        out = null;
      }
    }
  }

  /**
   * Read KeyBuffer/ValueBuffer pairs from a RCFile.
   * 
   */
  public static class Reader {

    private final Path file;
    private final FSDataInputStream in;

    private byte version;

    private CompressionCodec codec = null;
    private Metadata metadata = null;

    private final byte[] sync = new byte[SYNC_HASH_SIZE];
    private final byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;
    private long lastSeenSyncPos = 0;

    private long headerEnd;
    private final long end;
    private int currentKeyLength;
    private int currentRecordLength;

    private final Configuration conf;

    private final ValueBuffer currentValue;

    private boolean[] skippedColIDs = null;

    private int readRowsIndexInBuffer = 0;

    private int recordsNumInValBuffer = 0;

    private int columnNumber = 0;

    private int loadColumnNum;

    private int passedRowsNum = 0;

    private int[] columnRowReadIndex = null;
    private final NonSyncDataInputBuffer[] colValLenBufferReadIn;
    private final int[] columnRunLength;
    private final int[] columnPrvLength;
    private boolean decompress = false;

    private Decompressor keyDecompressor;
    NonSyncDataOutputBuffer keyDecompressedData = new NonSyncDataOutputBuffer();

    int[] prjColIDs = null; // selected column IDs

    /** Create a new RCFile reader. */
    public Reader(FileSystem fs, Path file, Configuration conf) throws IOException {
      this(fs, file, conf.getInt("io.file.buffer.size", 4096), conf, 0, fs
          .getFileStatus(file).getLen());
    }

    /** Create a new RCFile reader. */
    public Reader(FileSystem fs, Path file, int bufferSize, Configuration conf,
        long start, long length) throws IOException {
      conf.setInt("io.file.buffer.size", bufferSize);
      this.file = file;
      in = openFile(fs, file, bufferSize, length);
      this.conf = conf;
      end = start + length;
      boolean succeed = false;
      try {
        if (start > 0) {
          seek(0);
          init();
          seek(start);
        } else {
          init();
        }
        succeed = true;
      } finally {
        if (!succeed) {
          if (in != null) {
            try {
              in.close();
            } catch(IOException e) {
              if (LOG != null && LOG.isDebugEnabled()) {
                LOG.debug("Exception in closing " + in, e);
              }
            }
          }
        }
      }

      columnNumber = Integer.parseInt(metadata.get(
          new Text(COLUMN_NUMBER_METADATA_STR)).toString());

      java.util.ArrayList<Integer> notSkipIDs = ColumnProjectionUtils
          .getReadColumnIDs(conf);
      skippedColIDs = new boolean[columnNumber];
      if (notSkipIDs.size() > 0) {
        for (int i = 0; i < skippedColIDs.length; i++) {
          skippedColIDs[i] = true;
        }
        for (int read : notSkipIDs) {
          if (read < columnNumber) {
            skippedColIDs[read] = false;
          }
        }
      } else {
        // TODO: if no column name is specified e.g, in select count(1) from tt;
        // skip all columns, this should be distinguished from the case:
        // select * from tt;
        for (int i = 0; i < skippedColIDs.length; i++) {
          skippedColIDs[i] = false;
        }
      }

      loadColumnNum = columnNumber;
      if (skippedColIDs != null && skippedColIDs.length > 0) {
        for (boolean skippedColID : skippedColIDs) {
          if (skippedColID) {
            loadColumnNum -= 1;
          }
        }
      }

      // get list of selected column IDs
      prjColIDs = new int[loadColumnNum];
      for (int i = 0, j = 0; i < columnNumber; ++i) {
        if (!skippedColIDs[i]) {
          prjColIDs[j++] = i;
        }
      }

      colValLenBufferReadIn = new NonSyncDataInputBuffer[columnNumber];
      columnRunLength = new int[columnNumber];
      columnPrvLength = new int[columnNumber];
      columnRowReadIndex = new int[columnNumber];
      for (int i = 0; i < columnNumber; i++) {
        columnRowReadIndex[i] = 0;
        if (!skippedColIDs[i]) {
          colValLenBufferReadIn[i] = new NonSyncDataInputBuffer();
        }
        columnRunLength[i] = 0;
        columnPrvLength[i] = -1;
      }

      currentKey = createKeyBuffer();
      currentValue = new ValueBuffer(null, columnNumber, skippedColIDs, codec);
    }
    
    /**
     * Override this method to specialize the type of
     * {@link FSDataInputStream} returned.
     */
    protected FSDataInputStream openFile(FileSystem fs, Path file,
        int bufferSize, long length) throws IOException {
      return fs.open(file, bufferSize);
    }

    private void init() throws IOException {
      byte[] versionBlock = new byte[VERSION.length];
      in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1])
          || (versionBlock[2] != VERSION[2])) {
        throw new IOException(file + " not a RCFile");
      }

      // Set 'version'
      version = versionBlock[3];
      if (version > VERSION[3]) {
        throw new VersionMismatchException(VERSION[3], version);
      }

      try {
        Class<?> keyCls = conf.getClassByName(Text.readString(in));
        Class<?> valCls = conf.getClassByName(Text.readString(in));
        if (!keyCls.equals(KeyBuffer.class)
            || !valCls.equals(ValueBuffer.class)) {
          throw new IOException(file + " not a RCFile");
        }
      } catch (ClassNotFoundException e) {
        throw new IOException(file + " not a RCFile", e);
      }

      if (version > 2) { // if version > 2
        decompress = in.readBoolean(); // is compressed?
      } else {
        decompress = false;
      }

      // is block-compressed? it should be always false.
      boolean blkCompressed = in.readBoolean();
      if (blkCompressed) {
        throw new IOException(file + " not a RCFile.");
      }

      // setup the compression codec
      if (decompress) {
        String codecClassname = Text.readString(in);
        try {
          Class<? extends CompressionCodec> codecClass = conf.getClassByName(
              codecClassname).asSubclass(CompressionCodec.class);
          codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
              conf);
        } catch (ClassNotFoundException cnfe) {
          throw new IllegalArgumentException(
              "Unknown codec: " + codecClassname, cnfe);
        }
        keyDecompressor = CodecPool.getDecompressor(codec);
      }

      metadata = new Metadata();
      if (version >= VERSION_WITH_METADATA) { // if version >= 6
        metadata.readFields(in);
      }

      if (version > 1) { // if version > 1
        in.readFully(sync); // read sync bytes
        headerEnd = in.getPos();
      }
    }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return in.getPos();
    }

    /**
     * Set the current byte position in the input file.
     * 
     * <p>
     * The position passed must be a position returned by
     * {@link RCFile.Writer#getLength()} when writing this file. To seek to an
     * arbitrary position, use {@link RCFile.Reader#sync(long)}. In another
     * words, the current seek can only seek to the end of the file. For other
     * positions, use {@link RCFile.Reader#sync(long)}.
     */
    public synchronized void seek(long position) throws IOException {
      in.seek(position);
    }

    /** Seek to the next sync mark past a given position. */
    public synchronized void sync(long position) throws IOException {
      if (position + SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      //this is to handle syn(pos) where pos < headerEnd.
      if (position < headerEnd) {
        // seek directly to first record
        in.seek(headerEnd);
        // note the sync marker "seen" in the header
        syncSeen = true;
        return;
      }

      try {
        seek(position + 4); // skip escape
        in.readFully(syncCheck);
        int syncLen = sync.length;
        for (int i = 0; in.getPos() < end; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (sync[j] != syncCheck[(i + j) % syncLen]) {
              break;
            }
          }
          if (j == syncLen) {
            in.seek(in.getPos() - SYNC_SIZE); // position before
            // sync
            return;
          }
          syncCheck[i % syncLen] = in.readByte();
        }
      } catch (ChecksumException e) { // checksum failure
        handleChecksumException(e);
      }
    }

    private void handleChecksumException(ChecksumException e) throws IOException {
      if (conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
        sync(getPosition() + conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    private KeyBuffer createKeyBuffer() {
      return new KeyBuffer(columnNumber);
    }

    @SuppressWarnings("unused")
    private ValueBuffer createValueBuffer(KeyBuffer key) throws IOException {
      return new ValueBuffer(key, skippedColIDs);
    }

    /**
     * Read and return the next record length, potentially skipping over a sync
     * block.
     * 
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized int readRecordLength() throws IOException {
      if (in.getPos() >= end) {
        return -1;
      }
      int length = in.readInt();
      if (version > 1 && sync != null && length == SYNC_ESCAPE) { // process
        // a
        // sync entry
        lastSeenSyncPos = in.getPos() - 4; // minus SYNC_ESCAPE's length
        in.readFully(syncCheck); // read syncCheck
        if (!Arrays.equals(sync, syncCheck)) {
          throw new IOException("File is corrupt!");
        }
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt(); // re-read length
      } else {
        syncSeen = false;
      }
      return length;
    }

    private void seekToNextKeyBuffer() throws IOException {
      if (!keyInit) {
        return;
      }
      if (!currentValue.inited) {
        in.skip(currentRecordLength - currentKeyLength);
      }
    }

    private int compressedKeyLen = 0;
    NonSyncDataInputBuffer keyDataIn = new NonSyncDataInputBuffer();
    NonSyncDataInputBuffer keyDecompressBuffer = new NonSyncDataInputBuffer();
    NonSyncDataOutputBuffer keyTempBuffer = new NonSyncDataOutputBuffer();

    KeyBuffer currentKey = null;
    boolean keyInit = false;

    protected int nextKeyBuffer() throws IOException {
      seekToNextKeyBuffer();
      currentRecordLength = readRecordLength();
      if (currentRecordLength == -1) {
        keyInit = false;
        return -1;
      }
      currentKeyLength = in.readInt();
      compressedKeyLen = in.readInt();
      if (decompress) {
        keyTempBuffer.reset();
        keyTempBuffer.write(in, compressedKeyLen);
        keyDecompressBuffer.reset(keyTempBuffer.getData(), compressedKeyLen);
        CompressionInputStream deflatFilter = codec.createInputStream(
            keyDecompressBuffer, keyDecompressor);
        DataInputStream compressedIn = new DataInputStream(deflatFilter);
        deflatFilter.resetState();
        keyDecompressedData.reset();
        keyDecompressedData.write(compressedIn, currentKeyLength);
        keyDataIn.reset(keyDecompressedData.getData(), currentKeyLength);
        currentKey.readFields(keyDataIn);
      } else {
        currentKey.readFields(in);
      }

      keyInit = true;
      currentValue.inited = false;

      readRowsIndexInBuffer = 0;
      recordsNumInValBuffer = currentKey.numberRows;

      for (int prjColID : prjColIDs) {
        int i = prjColID;
        colValLenBufferReadIn[i].reset(currentKey.allCellValLenBuffer[i]
            .getData(), currentKey.allCellValLenBuffer[i].getLength());
        columnRowReadIndex[i] = 0;
        columnRunLength[i] = 0;
        columnPrvLength[i] = -1;
      }

      return currentKeyLength;
    }

    protected void currentValueBuffer() throws IOException {
      if (!keyInit) {
        nextKeyBuffer();
      }
      currentValue.keyBuffer = currentKey;
      currentValue.clearColumnBuffer();
      currentValue.readFields(in);
      currentValue.inited = true;
    }

    private boolean rowFetched = false;

    // use this buffer to hold column's cells value length for usages in
    // getColumn(), instead of using colValLenBufferReadIn directly.
    private final NonSyncDataInputBuffer fetchColumnTempBuf = new NonSyncDataInputBuffer();

    /**
     * Fetch all data in the buffer for a given column. This is useful for
     * columnar operators, which perform operations on an array data of one
     * column. It should be used together with {@link #nextColumnsBatch()}.
     * Calling getColumn() with not change the result of
     * {@link #next(LongWritable)} and
     * {@link #getCurrentRow(BytesRefArrayWritable)}.
     * 
     * @param columnID
     * @throws IOException
     */
    public BytesRefArrayWritable getColumn(int columnID,
        BytesRefArrayWritable rest) throws IOException {

      if (skippedColIDs[columnID]) {
        return null;
      }

      if (rest == null) {
        rest = new BytesRefArrayWritable();
      }

      rest.resetValid(recordsNumInValBuffer);

      if (!currentValue.inited) {
        currentValueBuffer();
      }

      int columnNextRowStart = 0;
      fetchColumnTempBuf.reset(currentKey.allCellValLenBuffer[columnID]
          .getData(), currentKey.allCellValLenBuffer[columnID].getLength());
      for (int i = 0; i < recordsNumInValBuffer; i++) {
        int length = getColumnNextValueLength(columnID);

        BytesRefWritable currentCell = rest.get(i);
        if (currentValue.decompressedFlag[columnID]) {
          currentCell.set(currentValue.loadedColumnsValueBuffer[columnID]
              .getData(), columnNextRowStart, length);
        } else {
          currentCell.set(currentValue.lazyDecompressCallbackObjs[columnID],
              columnNextRowStart, length);
        }
        columnNextRowStart = columnNextRowStart + length;
      }
      return rest;
    }

    /**
     * Read in next key buffer and throw any data in current key buffer and
     * current value buffer. It will influence the result of
     * {@link #next(LongWritable)} and
     * {@link #getCurrentRow(BytesRefArrayWritable)}
     * 
     * @return whether there still has records or not
     * @throws IOException
     */
    public synchronized boolean nextColumnsBatch() throws IOException {
      passedRowsNum += (recordsNumInValBuffer - readRowsIndexInBuffer);
      return nextKeyBuffer() > 0;
    }

    /**
     * Returns how many rows we fetched with next(). It only means how many rows
     * are read by next(). The returned result may be smaller than actual number
     * of rows passed by, because {@link #seek(long)},
     * {@link #nextColumnsBatch()} can change the underlying key buffer and
     * value buffer.
     * 
     * @return next row number
     * @throws IOException
     */
    public synchronized boolean next(LongWritable readRows) throws IOException {
      if (hasRecordsInBuffer()) {
        readRows.set(passedRowsNum);
        readRowsIndexInBuffer++;
        passedRowsNum++;
        rowFetched = false;
        return true;
      } else {
        keyInit = false;
      }

      int ret = -1;
      try {
        ret = nextKeyBuffer();
      } catch (EOFException eof) {
        eof.printStackTrace();
      }
      if (ret > 0) {
        return next(readRows);
      }
      return false;
    }

    public boolean hasRecordsInBuffer() {
      return readRowsIndexInBuffer < recordsNumInValBuffer;
    }

    /**
     * get the current row used,make sure called {@link #next(LongWritable)}
     * first.
     * 
     * @throws IOException
     */
    public synchronized void getCurrentRow(BytesRefArrayWritable ret) throws IOException {

      if (!keyInit || rowFetched) {
        return;
      }

      if (!currentValue.inited) {
        currentValueBuffer();
        // do this only when not initialized, but we may need to find a way to
        // tell the caller how to initialize the valid size
        ret.resetValid(columnNumber);
      }

      // we do not use BytesWritable here to avoid the byte-copy from
      // DataOutputStream to BytesWritable

      for (int j = 0; j < prjColIDs.length; ++j) {
        int i = prjColIDs[j];

        BytesRefWritable ref = ret.unCheckedGet(i);

        int columnCurrentRowStart = columnRowReadIndex[i];
        int length = getColumnNextValueLength(i);
        columnRowReadIndex[i] = columnCurrentRowStart + length;

        if (currentValue.decompressedFlag[j]) {
          ref.set(currentValue.loadedColumnsValueBuffer[j].getData(),
              columnCurrentRowStart, length);
        } else {
          ref.set(currentValue.lazyDecompressCallbackObjs[j],
              columnCurrentRowStart, length);
        }
      }
      rowFetched = true;
    }

    private int getColumnNextValueLength(int i) throws IOException {
      if (columnRunLength[i] > 0) {
        --columnRunLength[i];
        return columnPrvLength[i];
      } else {
        int length = (int) WritableUtils.readVLong(colValLenBufferReadIn[i]);
        if (length < 0) {
          // we reach a runlength here, use the previous length and reset
          // runlength
          columnRunLength[i] = ~length;
          columnRunLength[i]--;
          length = columnPrvLength[i];
        } else {
          columnPrvLength[i] = length;
          columnRunLength[i] = 0;
        }
        return length;
      }
    }

    /** Returns true iff the previous call to next passed a sync mark. */
    public boolean syncSeen() {
      return syncSeen;
    }

    /** Returns the last seen sync position. */
    public long lastSeenSyncPos() {
      return lastSeenSyncPos;
    }

    /** Returns the name of the file. */
    @Override
    public String toString() {
      return file.toString();
    }

    /** Close the reader. */
    public void close() {
      IOUtils.closeStream(in);
      currentValue.close();
      if (decompress) {
        IOUtils.closeStream(keyDecompressedData);
        CodecPool.returnDecompressor(keyDecompressor);
      }
    }
  }
}
