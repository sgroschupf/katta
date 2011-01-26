/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.util;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Helper class for dealing with hadoop writable <-> java primitive wrapper
 * conversion.
 * 
 * @see Writable
 */
public enum WritableType {

  TEXT, BYTE, INT, LONG, FLOAT, DOUBLE;

  public static WritableType detectWritableType(Comparable comparable) {
    if (comparable instanceof Byte) {
      return WritableType.BYTE;
    } else if (comparable instanceof Integer) {
      return WritableType.INT;
    } else if (comparable instanceof String) {
      return WritableType.TEXT;
    } else if (comparable instanceof Float) {
      return WritableType.FLOAT;
    } else if (comparable instanceof Long) {
      return WritableType.LONG;
    } else if (comparable instanceof Double) {
      return WritableType.DOUBLE;
    }
    throw new IllegalArgumentException("no conversion rule for comparable of type " + comparable.getClass().getName());
  }

  public static WritableType[] detectWritableTypes(Comparable[] comparables) {
    WritableType[] writablTypes = new WritableType[comparables.length];
    for (int i = 0; i < comparables.length; i++) {
      writablTypes[i] = detectWritableType(comparables[i]);
    }
    return writablTypes;
  }

  public WritableComparable newWritableComparable() {
    switch (this) {
    case TEXT:
      return new Text();
    case BYTE:
      return new ByteWritable();
    case INT:
      return new IntWritable();
    case LONG:
      return new LongWritable();
    case FLOAT:
      return new FloatWritable();
    case DOUBLE:
      return new DoubleWritable();
    }
    throw getUnhandledTypeException();
  }

  /**
   * Convert a java primitive type wrapper (like String, Integer, Float, etc...)
   * to the corresponding hadoop {@link WritableComparable}.
   */
  public WritableComparable convertComparable(Comparable comparable) {
    switch (this) {
    case TEXT:
      return new Text((String) comparable);
    case BYTE:
      return new ByteWritable(((Byte) comparable).byteValue());
    case INT:
      return new IntWritable(((Integer) comparable).intValue());
    case LONG:
      return new LongWritable((((Long) comparable).longValue()));
    case FLOAT:
      return new FloatWritable(((Float) comparable).floatValue());
    case DOUBLE:
      return new DoubleWritable(((Double) comparable).doubleValue());
    }
    throw getUnhandledTypeException();
  }

  public static WritableComparable[] convertComparable(WritableType[] writableTypes, Comparable[] comparables) {
    WritableComparable[] writableComparables = new WritableComparable[comparables.length];
    for (int i = 0; i < writableComparables.length; i++) {
      writableComparables[i] = writableTypes[i].convertComparable(comparables[i]);

    }
    return writableComparables;
  }

  private RuntimeException getUnhandledTypeException() {
    return new IllegalArgumentException("type " + this + " not handled");
  }

}
