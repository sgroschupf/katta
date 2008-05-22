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
import java.util.Set;

public class NameNodeTest extends AbstractIndexes {

  private static NameNode n = null;

  private static Lease[] leases = null;

  protected void setUp() throws Exception {
    super.setUp();
    if (n == null) {
      n = new NameNode(conf, new InetSocketAddress(Constants.HOST, TestUtils
          .getNextPort()));
    }
    for (int i = 0; i <= 2; i++) {
      toSet(n.heartbeat(getStatus()[i], getSetup()[i], leases));
    }
  }

  public void testGetProtocolVersion() throws Exception {
    assertTrue(n
        .getProtocolVersion(ClientToNameNodeProtocol.class.getName(), 0) == ClientToNameNodeProtocol.VERSION_ID);
    assertTrue(n.getProtocolVersion(DataNodeToNameNodeProtocol.class.getName(),
        0) == DataNodeToNameNodeProtocol.VERSION_ID);
  }

  public void testGetProtocolVersionNull() {
    try {
      n.getProtocolVersion(DataNodeToDataNodeProtocol.class.getName(), 0);
      fail("Should throw an exception");
    } catch (IOException io) {
      //
    }
  }

  public void testGetSearchableIndexes() throws Exception {
    Set<IndexLocation> indexes = toSet(n.getSearchableIndexes());
    assertEquals(3, indexes.size());
    assertTrue(indexes.contains(getSetup()[0][0]));
    assertTrue(indexes.contains(getSetup()[0][1]));
    assertTrue(indexes.contains(getSetup()[2][0]));
  }

  public void testHeartbeat() throws Exception {
    for (int i = 0; i <= 2; i++) {
      assert (toSet(n.heartbeat(getStatus()[i], getSetup()[i], leases)) == null);
    }
    n.doHeartbeat();
    assertTrue(toSet(n.heartbeat(getStatus()[0], getSetup()[0], leases)) != null
        || toSet(n.heartbeat(getStatus()[1], getSetup()[1], leases)) != null
        || toSet(n.heartbeat(getStatus()[2], getSetup()[2], leases)) != null);
  }

  public void testOne() throws Exception {

    // add two indexes to the datanode
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation ilA = new IndexLocation(getStatus()[0].getAddress(), version,
        IndexState.LIVE);
    IndexLocation ilB = new IndexLocation(getStatus()[0].getAddress(), version
        .nextVersion(), IndexState.LIVE);
    IndexLocation[] searchableIndexes = { ilA, ilB };

    // send a heartbeat
    HeartbeatResponse result = n
        .heartbeat(getStatus()[0], searchableIndexes, leases);

    // result should be null because we have not built a replication plan yet
    assertNull(result.getReplicationRequests());

    // build the replication plan
    n.doHeartbeat();
    result = n.heartbeat(getStatus()[0], searchableIndexes, leases);

    // result should still be null because we only have a single datanode
    assertNull(result.getReplicationRequests());

    // send a heartbeat
    result = n.heartbeat(getStatus()[1], empty, leases);
    result = n.heartbeat(getStatus()[2], empty, leases);
    n.doHeartbeat();
    result = n.heartbeat(getStatus()[0], searchableIndexes, leases);
    result = n.heartbeat(getStatus()[1], searchableIndexes, leases);
  }

  public void testToString() {
    assertNotNull(n.toString());
  }
}
