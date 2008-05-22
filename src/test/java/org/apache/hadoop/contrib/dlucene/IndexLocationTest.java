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

import java.net.InetSocketAddress;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;

public class IndexLocationTest extends TestUtils {

  private static String L_INDEX_ONE = getNextIndex();
  private IndexLocation location = null;
  private InetSocketAddress addr = null;
  private IndexVersion iv = null;

  protected void setUp() throws Exception {
    super.setUp();
    addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
    iv = new IndexVersion(L_INDEX_ONE);
    location = new IndexLocation(addr, iv, IndexState.LIVE);
  }

  public void testNullArguments() throws Exception {
    try {
      location.write(null);
      fail("write() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      location.readFields(null);
      fail("readFields() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      new IndexLocation(addr, (IndexVersion) null, IndexState.LIVE);
      fail("constructor() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      IndexVersion iv1 = new IndexVersion(L_INDEX_ONE);
      new IndexLocation((InetSocketAddress) null, iv1, IndexState.LIVE);
      fail("constructor() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void testHashCode() {
    iv = iv.nextVersion();
    IndexLocation b = new IndexLocation(addr, iv, IndexState.LIVE);
    assertTrue(location.hashCode() != b.hashCode());
  }

  public void testGetIndexVersion() {
    assertEquals(L_INDEX_ONE, location.getIndexVersion().getName());
    assertEquals(0, location.getIndexVersion().getVersion());
  }

  public void testGetLocation() {
    assertEquals(addr, location.getAddress());
  }

  public void testEqualsObject() {
    assertEquals(location, location);
    iv = iv.nextVersion();
    IndexLocation b = new IndexLocation(addr, iv, IndexState.LIVE);
    assertFalse(location.equals(b));
  }

  public void testWriteAndRead() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    location.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    IndexLocation four = IndexLocation.read(in);
    assertEquals(four, location);
  }

  public void testGetState() {
    assertEquals(IndexState.LIVE, location.getState());
  }

  public void testSetState() {
    location.setState(IndexState.UNCOMMITTED);
    assertEquals(IndexState.UNCOMMITTED, location.getState());
  }

  public void testIndexLocationIndexVersionInetSocketAddressIndexState() {
    IndexVersion iv1 = new IndexVersion(L_INDEX_ONE);
    location = new IndexLocation(addr, iv1, IndexState.LIVE);
  }

  public void testNextVersion() {
    IndexLocation indexTwo = new IndexLocation(location.getAddress(), location
        .getIndexVersion().nextVersion(), IndexState.LIVE);
    assertEquals(location.getAddress(), indexTwo.getAddress());
    assertEquals(location.getIndexVersion().getName(), indexTwo
        .getIndexVersion().getName());
    assertTrue(indexTwo.getIndexVersion().getVersion() > location
        .getIndexVersion().getVersion());
    assertTrue(indexTwo.compareTo(location) < 0);
  }

}
