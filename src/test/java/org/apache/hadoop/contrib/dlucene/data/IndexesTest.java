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

package org.apache.hadoop.contrib.dlucene.data;

import java.net.InetSocketAddress;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.net.NetUtils;

public class IndexesTest extends TestUtils {

  private Indexes indexes = null;
  private InetSocketAddress addr1 = null;
  private InetSocketAddress addr2 = null;
  private static String NN_INDEX_ONE = getNextIndex();

  protected void setUp() throws Exception {
    super.setUp();
    indexes = new Indexes();
    addr1 = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
    addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
  }

  public void testCommitIndex() throws Exception {
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation location = new IndexLocation(addr1, version,
        IndexState.UNCOMMITTED);
    indexes.add(location, Network.DEFAULT_RACK);
    assertEquals(location, indexes.getUncommittedIndex(NN_INDEX_ONE));
    indexes.commitIndex(NN_INDEX_ONE);
    assertEquals(null, indexes.getUncommittedIndex(NN_INDEX_ONE));
  }

  public void testGetPrimaryIndex() {
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation location1 = new IndexLocation(addr1, version, IndexState.LIVE);
    indexes.add(location1, Network.DEFAULT_RACK);
    assertEquals(location1.getIndexVersion(), indexes.getPrimaryIndex(NN_INDEX_ONE));
    IndexLocation location2 = new IndexLocation(addr1, version.nextVersion(),
        IndexState.LIVE);
    indexes.add(location2, Network.DEFAULT_RACK);
    assertEquals(location2.getIndexVersion(), indexes.getPrimaryIndex(NN_INDEX_ONE));
  }

  public void testGetReplicas() {
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation location1 = new IndexLocation(addr1, version, IndexState.LIVE);
    indexes.add(location1, Network.DEFAULT_RACK);
    IndexLocation location2 = new IndexLocation(addr2, version, IndexState.LIVE);
    indexes.add(location2, Network.DEFAULT_RACK);
    Set<InetSocketAddress> addresses = indexes.getReplicaAddresses(version);
    Set<String> racks = indexes.getReplicaRacks(version);
    assertTrue(racks.contains(Network.DEFAULT_RACK));
    assertTrue(addresses.contains(addr1));
    assertTrue(addresses.contains(addr2));
  }

  public void testRemoveIndex() {
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation location1 = new IndexLocation(addr1, version,
        IndexState.UNCOMMITTED);
    indexes.add(location1, Network.DEFAULT_RACK);
    assertTrue(indexes.getIndexes().contains(version));
    indexes.remove(location1, Network.DEFAULT_RACK);
    assertTrue(!indexes.getIndexes().contains(version));
    // assertTrue(!indexes.getPrimaryIndex(NN_INDEX_ONE).equals(location1));
    assertNull(indexes.getUncommittedIndex(NN_INDEX_ONE));
  }
}
