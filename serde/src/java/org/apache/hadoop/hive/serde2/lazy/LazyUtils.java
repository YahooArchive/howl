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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyUtils {

  /**
   * Returns the digit represented by character b.
   * @param b  The ascii code of the character
   * @param radix  The radix
   * @return  -1 if it's invalid
   */
  public static int digit(int b, int radix) {
    int r = -1;
    if (b >= '0' && b<='9') {
      r = b - '0';
    } else if (b >= 'A' && b<='Z') {
      r = b - 'A' + 10;
    } else if (b >= 'a' && b <= 'z') {
      r = b - 'a' + 10;
    }
    if (r >= radix) r = -1;
    return r;
  }
  
  /**
   * Returns -1 if the first byte sequence is lexicographically less than the second;
   * returns +1 if the second byte sequence is lexicographically less than the first;
   * otherwise return 0.
   */
  public static int compare(byte[] b1, int start1, int length1, byte[] b2, int start2, int length2) {
    
    int min = Math.min(length1, length2);
    
    for (int i = 0; i < min; i++) {
      if (b1[start1 + i] == b2[start2 + i]) {
        continue;
      }
      if (b1[start1 + i] < b2[start2 + i]) {
        return -1;
      } else {
        return 1;
      }      
    }
    
    if (length1 < length2) return -1;
    if (length1 > length2) return 1;
    return 0;
  }
  
  /**
   * Convert a UTF-8 byte array to String.
   * @param bytes  The byte[] containing the UTF-8 String.
   * @param start  The start position inside the bytes.
   * @param length The length of the data, starting from "start"
   * @return The unicode String
   */
  public static String convertToString(byte[] bytes, int start, int length) {
    try {
      return Text.decode(bytes, start, length);
    } catch (CharacterCodingException e) {
      return null;
    }
  }

  /**
   * Write out the text representation of a Primitive Object to a UTF8 byte stream. 
   * @param out  The UTF8 byte OutputStream
   * @param o    The primitive Object
   */
  public static void writePrimitiveUTF8(OutputStream out, Object o, PrimitiveObjectInspector oi) throws IOException {
    
    switch (oi.getPrimitiveCategory()) {
      case BYTE: {
        LazyInteger.writeUTF8(out, ((ByteObjectInspector)oi).get(o));
        break;
      }
      case SHORT: {
        LazyInteger.writeUTF8(out, ((ShortObjectInspector)oi).get(o));
        break;
      }
      case INT: {
        LazyInteger.writeUTF8(out, ((IntObjectInspector)oi).get(o));
        break;
      }
      case LONG: {
        LazyLong.writeUTF8(out, ((LongObjectInspector)oi).get(o));
        break;
      }
      // TODO: We should enable this piece of code, once we pass ObjectInspector in the Operator.init() 
      // instead of Operator.forward().  Until then, JoinOperator will assume the output columns are
      // all strings but they may not be.
      /*
      case STRING: {
        Text t = ((StringObjectInspector)oi).getPrimitiveWritableObject(o);
        out.write(t.getBytes(), 0, t.getLength());
        break;
      }
      */
      default: {
        if (o instanceof Text) {
          // This piece of code improves the performance because we don't need to
          // convert Text to String then back to Text.  We should rely on the code
          // block above when JoinOperator is fixed.
          Text t = (Text)o;
          out.write(t.getBytes(), 0, t.getLength());
        } else {
          ByteBuffer b = Text.encode(o.toString());
          out.write(b.array(), 0, b.limit());
        }
      }
    }
  }
  
}
