package org.apache.hadoop.hive.howl.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DefaultHowlRecord implements HowlRecord {

  private final List<Object> contents;

  public DefaultHowlRecord(){
    contents = new ArrayList<Object>();
  }

  public DefaultHowlRecord(int size){
    contents = new ArrayList<Object>(size);
    for(int i=0; i < size; i++){
      contents.add(null);
    }
  }

  public DefaultHowlRecord(List<Object> list) {
    contents = list;
  }

  @Override
  public Object get(int fieldNum) {
    return contents.get(fieldNum);
  }

  @Override
  public List<Object> getAll() {
    return contents;
  }

  @Override
  public void set(int fieldNum, Object val) {
    contents.set(fieldNum, val);
  }

  @Override
  public int size() {
    return contents.size();
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    contents.clear();
    int len = in.readInt();
    for(int i =0; i < len; i++){
      contents.add(ReaderWriter.readDatum(in));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int sz = size();
    out.writeInt(sz);
    for (int i = 0; i < sz; i++) {
      ReaderWriter.writeDatum(out, contents.get(i));
    }

  }

  @Override
  public int compareTo(Object that) {

    if(that instanceof HowlRecord) {
      HowlRecord other = (HowlRecord)that;
      int mySz = this.size();
      int urSz = other.size();
      if(mySz != urSz) {
        return mySz - urSz;
      } else{
        for (int i = 0; i < mySz;i++) {
          int c = DataType.compare(get(i), other.get(i));
          if (c != 0) {
            return c;
          }
        }
      }
      return 0;
    } else {
      return DataType.compare(this, that);
    }
  }

  @Override
  public boolean equals(Object other) {
    return (compareTo(other) == 0);
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for (Object o : contents) {
      if (o != null) {
        hash = 31 * hash + o.hashCode();
      }
    }
    return hash;
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();
    for(Object o : contents) {
      sb.append(o+"\t");
    }
    return sb.toString();
  }
}
