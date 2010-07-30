package org.apache.hadoop.hive.howl.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

public class HowlArrayBag<T> implements DataBag {

  private static final long DUMMY_SIZE = 100;
  List<T>  rawItemList = null;
  DataBag convertedBag = null;
//  List<Tuple> tupleList = null;

  public class HowlArrayBagIterator implements Iterator<Tuple> {

    Iterator<T> iter = null;

    public HowlArrayBagIterator(List<T> rawItemList) {
      iter = rawItemList.iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Tuple next() {
      Tuple t = new DefaultTuple();
      t.append(iter.next());
      return t;
    }

    @Override
    public void remove() {
      iter.remove();
    }

  }

  private void convertFromRawToTupleForm(){
    if (convertedBag == null){
      convertedBag = new DefaultDataBag();
      for (T item : rawItemList){
        Tuple t = new DefaultTuple();
        t.append(item);
        convertedBag.add(t);
      }
    }else{
      // TODO : throw exception or be silent? Currently going with silence, but needs revisiting.
    }
  }

  @Override
  public void add(Tuple t) {
    if (convertedBag == null){
      convertFromRawToTupleForm();
    }
    convertedBag.add(t);
  }

  @Override
  public void addAll(DataBag db) {
    Tuple t;
    for (Iterator<Tuple> dbi = db.iterator() ; dbi.hasNext();){
      this.add(dbi.next());
    }
  }

  @Override
  public void clear() {
    rawItemList = null;
    if (convertedBag != null){
      convertedBag.clear();
      convertedBag = null;
    }
  }

  @Override
  public boolean isDistinct() {
    return false;
  }

  @Override
  public boolean isSorted() {
    return false;
  }

  @Override
  public Iterator<Tuple> iterator() {
    if (convertedBag != null){
      return convertedBag.iterator();
    }else{
      return new HowlArrayBagIterator(rawItemList);
    }
  }

  @Override
  public void markStale(boolean arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public long size() {
    return (convertedBag == null ? (rawItemList == null ? 0 : rawItemList.size()) : convertedBag.size() );
  }

  @Override
  public long getMemorySize() {
    // FIXME: put in actual impl
    return DUMMY_SIZE;
  }

  @Override
  public long spill() {
    // FIXME: put in actual spill impl
    return 0;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    convertedBag = new DefaultDataBag();
    convertedBag.readFields(arg0);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    convertFromRawToTupleForm();
    convertedBag.write(arg0);
  }

  @Override
  public int compareTo(Object arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

}
