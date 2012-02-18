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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.katta.AbstractTest;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class WritableTypeTest extends AbstractTest {

  private final static Comparable[] COMPARABLES;
  private final static WritableComparable[] CONVERTED_COMPARABLES;

  static {
    // filled corresponding with WritableTypes array
    List<Comparable> comparables = new ArrayList();
    comparables.add("as");
    comparables.add(new Byte((byte) 3));
    comparables.add(new Integer(3));
    comparables.add(new Long(3));
    comparables.add(new Float(3));
    comparables.add(new Double(3));
    COMPARABLES = comparables.toArray(new Comparable[comparables.size()]);

    // corresponds to the above list
    List<WritableComparable> writableComparables = new ArrayList();
    writableComparables.add(new Text("as"));
    writableComparables.add(new ByteWritable((byte) 3));
    writableComparables.add(new IntWritable(3));
    writableComparables.add(new LongWritable(3));
    writableComparables.add(new FloatWritable(3));
    writableComparables.add(new DoubleWritable(3));
    CONVERTED_COMPARABLES = writableComparables.toArray(new WritableComparable[writableComparables.size()]);
  }

  @Test
  public void testDetectWritableType() {
    for (int i = 0; i < COMPARABLES.length; i++) {
      assertEquals(WritableType.values()[i], WritableType.detectWritableType(COMPARABLES[i]));
    }
  }

  @Test
  public void testDetectWritableTypes() {
    WritableType[] writableTypes = WritableType.detectWritableTypes(COMPARABLES);
    for (int i = 0; i < writableTypes.length; i++) {
      assertEquals(WritableType.values()[i], writableTypes[i]);
    }
  }

  @Test
  public void testNewWritableComparable() {
    Comparable[] comparables = COMPARABLES;
    for (Comparable comparable : comparables) {
      assertNotNull(comparable);
    }
  }

  @Test
  public void testConvertComparable() {
    for (int i = 0; i < COMPARABLES.length; i++) {
      assertEquals(CONVERTED_COMPARABLES[i], WritableType.values()[i].convertComparable(COMPARABLES[i]));
    }
  }

  @Test
  public void testConvertComparables() {
    WritableComparable[] writableComparables = WritableType.convertComparable(WritableType.values(), COMPARABLES);
    for (int i = 0; i < writableComparables.length; i++) {
      assertEquals(CONVERTED_COMPARABLES[i], writableComparables[i]);
    }
  }

  @Test
  public void testUnhandledTypes() {
    try {
      WritableType.detectWritableType(new Date());
      fail("should throw exception");
    } catch (Exception e) {
      // expected
    }

  }

}
