package org.apache.hadoop.hive.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;

public class TestDefaultHowlRecord extends TestCase{

  public void testRYW() throws IOException{

    File f = new File("binary.dat");
    f.delete();
    f.createNewFile();
    f.deleteOnExit();

    OutputStream fileOutStream = new FileOutputStream(f);
    DataOutput outStream = new DataOutputStream(fileOutStream);

    HowlRecord[]  recs = getHowlRecords();
    for(int i =0; i < recs.length; i++){
      recs[i].write(outStream);
    }
    fileOutStream.flush();
    fileOutStream.close();

    InputStream fInStream = new FileInputStream(f);
    DataInput inpStream = new DataInputStream(fInStream);

    for(int i =0; i < recs.length; i++){
      HowlRecord rec = new DefaultHowlRecord();
      rec.readFields(inpStream);
      Assert.assertEquals(recs[i],rec);
    }

    Assert.assertEquals(fInStream.available(), 0);
    fInStream.close();

  }

  public void testCompareTo() {
    HowlRecord[] recs = getHowlRecords();
    Assert.assertEquals(recs[0].compareTo(recs[1]),0);
  }

  public void testEqualsObject() {

    HowlRecord[] recs = getHowlRecords();
    Assert.assertTrue(recs[0].equals(recs[1]));
  }

  private HowlRecord[] getHowlRecords(){

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
    rec_2.add( new Byte("123"));
    rec_2.add( new Short("456"));
    rec_2.add( new Integer(789));
    rec_2.add( new Long(1000L));
    rec_2.add( new Double(5.3D));
    rec_2.add( new String("howl and hadoop"));
    rec_2.add( null);
    rec_2.add( "null");
    HowlRecord tup_2 = new DefaultHowlRecord(rec_2);

    return  new HowlRecord[]{tup_1,tup_2};

  }
}
