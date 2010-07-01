package org.apache.hadoop.hive.howl.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public abstract class ReaderWriter {

  private static final String UTF8 = "UTF-8";

  public static Object readDatum(DataInput in) throws IOException {

    byte type = in.readByte();
    switch (type) {

    case DataType.STRING:
      byte[] buffer = new byte[in.readInt()];
      in.readFully(buffer);
      return new String(buffer,UTF8);

    case DataType.INTEGER:
      return in.readInt();

    case DataType.LONG:
      return in.readLong();

    case DataType.FLOAT:
      return in.readFloat();

    case DataType.DOUBLE:
      return in.readDouble();

    case DataType.BOOLEAN:
      return in.readBoolean();

    case DataType.BYTE:
      return in.readByte();

    case DataType.SHORT:
      return in.readShort();

    case DataType.NULL:
      return null;

    case DataType.MAP:
      int size = in.readInt();
      Map<Object,Object> m = new HashMap<Object, Object>(size);
      for (int i = 0; i < size; i++) {
          m.put(readDatum(in), readDatum(in));
      }
      return m;

    case DataType.LIST:
      int sz = in.readInt();
      List<Object> list = new ArrayList<Object>(sz);
      for(int i=0; i < sz; i++) {
        list.add(readDatum(in));
      }
      return list;

    default:
      throw new IOException("Unexpected data type " + type +
          " found in stream.");
    }
  }

  public static void writeDatum(DataOutput out, Object val) throws IOException {
    // Read the data type
    byte type = DataType.findType(val);
    switch (type) {
    case DataType.LIST:
      out.writeByte(DataType.LIST);
      List<?> list = (List<?>)val;
      int sz = list.size();
      out.writeInt(sz);
      for (int i = 0; i < sz; i++) {
        writeDatum(out, list.get(i));
      }
      return;

    case DataType.MAP:
      out.writeByte(DataType.MAP);
      Map<?,?> m = (Map<?, ?>)val;
      out.writeInt(m.size());
      Iterator<?> i =
        m.entrySet().iterator();
      while (i.hasNext()) {
        Entry<?,?> entry = (Entry<?, ?>) i.next();
        writeDatum(out, entry.getKey());
        writeDatum(out, entry.getValue());
      }
      return;

    case DataType.INTEGER:
      out.writeByte(DataType.INTEGER);
      out.writeInt((Integer)val);
      return;

    case DataType.LONG:
      out.writeByte(DataType.LONG);
      out.writeLong((Long)val);
      return;

    case DataType.FLOAT:
      out.writeByte(DataType.FLOAT);
      out.writeFloat((Float)val);
      return;

    case DataType.DOUBLE:
      out.writeByte(DataType.DOUBLE);
      out.writeDouble((Double)val);
      return;

    case DataType.BOOLEAN:
      out.writeByte(DataType.BOOLEAN);
      out.writeBoolean((Boolean)val);
      return;

    case DataType.BYTE:
      out.writeByte(DataType.BYTE);
      out.writeByte((Byte)val);
      return;

    case DataType.SHORT:
      out.writeByte(DataType.SHORT);
      out.writeShort((Short)val);
      return;

    case DataType.STRING:
      String s = (String)val;
      byte[] utfBytes = s.getBytes(ReaderWriter.UTF8);
      out.writeByte(DataType.STRING);
      out.writeInt(utfBytes.length);
      out.write(utfBytes);
      return;


    case DataType.NULL:
      out.writeByte(DataType.NULL);
      return;

    default:
      throw new IOException("Unexpected data type " + type +
          " found in stream.");
    }
  }
}
