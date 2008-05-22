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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;

public class LeaseTest extends TestUtils {

  private static String IVT_INDEX_ONE = null;
  private static Lease lease = null;
  private static IndexVersion indexVersionOne = null;
  private static InetSocketAddress addr = null;
  private static long leaseLength = 1000;
  
  protected void setUp() throws Exception {
    super.setUp();
    if (IVT_INDEX_ONE == null) {
      IVT_INDEX_ONE = getNextIndex();
      indexVersionOne = new IndexVersion(IVT_INDEX_ONE);
      addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
      IndexLocation il = new IndexLocation(addr, indexVersionOne, IndexState.UNKNOWN);
      lease = new Lease(il, leaseLength);
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testLease() {
    assertEquals(addr, lease.getAddress());
    assertEquals(indexVersionOne, lease.getIndex());
  }

  public void testIsValid() {
    assertTrue(lease.isValid());
    long currentTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < currentTime + 2 * leaseLength) {
      // wait
    }
    assertFalse(lease.isValid());
    lease.renew();
    assertTrue(lease.isValid());
  }

  public void testReadAndWrite() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    lease.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    Lease readLease = Lease.read(in);
    assertEquals(lease, readLease);
  }
}
