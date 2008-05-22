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

public class HeartbeatResponseTest extends TestUtils {
  
  private static HeartbeatResponse hbr = null;
  private static String L_INDEX_ONE = getNextIndex();
  private static IndexLocation location = null;
  private static IndexVersion iv = null;
  private static Lease lease = null;
  private static IndexVersion indexVersionOne = null;
  private static InetSocketAddress addr = null;
  private static long leaseLength = 1000;

  protected void setUp() throws Exception {
    super.setUp();
    if (hbr == null) {
      addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
      iv = new IndexVersion(L_INDEX_ONE);
      location = new IndexLocation(addr, iv, IndexState.LIVE);
      addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
      lease = new Lease(location, leaseLength);
      IndexLocation[] il = { location };
      Lease[] l = { lease };
      hbr = new HeartbeatResponse(il, l);
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testGetFields() {
    IndexLocation[] il = hbr.getReplicationRequests();
    assertEquals(il[0], location);
    Lease[] l = hbr.getUpdatedLeases();
    assertEquals(l[0], lease);
  }

  
  public void testReadAndWrite() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    hbr.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    HeartbeatResponse hbrIn = HeartbeatResponse.read(in);
    assertEquals(hbr, hbrIn);
  }
}
