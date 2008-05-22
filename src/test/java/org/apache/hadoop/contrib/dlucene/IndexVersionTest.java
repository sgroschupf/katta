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

package org.apache.hadoop.contrib.dlucene;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class IndexVersionTest extends TestUtils {

  private static String IVT_INDEX_ONE = null;
  private static String IVT_INDEX_TWO = null;
  private static IndexVersion indexVersionOne = null;
  private static IndexVersion indexVersionTwo = null;
  private static IndexVersion indexVersionThree = null;

  protected void setUp() throws Exception {
    super.setUp();

    if (IVT_INDEX_ONE == null) {
      IVT_INDEX_ONE = getNextIndex();
      IVT_INDEX_TWO = getNextIndex();
      indexVersionOne = new IndexVersion(IVT_INDEX_ONE);
      indexVersionTwo = new IndexVersion(IVT_INDEX_TWO, 22);
      indexVersionThree = indexVersionOne.nextVersion();
    }
  }

  public void testNullArguments() throws Exception {
    try {
      new IndexVersion(null);
      fail("constructor should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      new IndexVersion((String) null);
      fail("constructor should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      IndexVersion.read((DataInput) null);
      fail("read() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      indexVersionOne.write(null);
      fail("write() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      indexVersionOne.readFields(null);
      fail("readFields() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void testHashCode() {
    assertTrue(indexVersionOne.hashCode() != indexVersionTwo.hashCode());
  }

  public void testNextVersion() {
    assertEquals(1, indexVersionThree.getVersion());
  }

  public void testGetName() {
    assertEquals(IVT_INDEX_ONE, indexVersionOne.getName());
    assertEquals(IVT_INDEX_TWO, indexVersionTwo.getName());
    assertEquals(IVT_INDEX_ONE, indexVersionThree.getName());
  }

  public void testGetVersion() {
    assertEquals(0, indexVersionOne.getVersion());
    assertEquals(22, indexVersionTwo.getVersion());
  }

  public void testEqualsObject() {
    assertEquals(indexVersionOne, indexVersionOne);
    assertFalse(indexVersionThree.equals(indexVersionOne));
    assertFalse(indexVersionTwo.equals(indexVersionOne));
  }

  public void testCompareTo() {
    assertEquals(1, indexVersionThree.getVersion());
    assertTrue(indexVersionThree.compareTo(indexVersionOne) < 0);
  }

  public void testWriteAndRead() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    indexVersionOne.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    IndexVersion four = IndexVersion.read(in);
    assertEquals(four, indexVersionOne);
  }
}
