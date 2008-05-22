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

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.net.NetUtils;

public class ReplicatedIndexesTest extends TestUtils {

  private static String L_INDEX_ONE = getNextIndex();
  private ReplicatedIndexes ri = null;
  private InetSocketAddress addr = null;
  private IndexLocation location = null;
  private IndexVersion iv = null;

  protected void setUp() throws Exception {
    super.setUp();
    ri = new ReplicatedIndexes();
    addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
    iv = new IndexVersion(L_INDEX_ONE);
    location = new IndexLocation(addr, iv, IndexState.LIVE);
  }

  public void testGetIndexes() {
    assertEquals(0, ri.getIndexes().size());
    ri.add(location, RACKS[0]);
    assertEquals(1, ri.getIndexes().size());
  }

  public void testAdd() {
    ri.add(location, RACKS[0]);
    assertEquals(1, ri.getIndexes().size());

    InetSocketAddress addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
    IndexLocation location2 = new IndexLocation(addr2, iv, IndexState.LIVE);
    ri.add(location2, RACKS[1]);
    assertEquals(1, ri.getIndexes().size());

    IndexVersion iv2 = iv.nextVersion();
    IndexLocation location3 = new IndexLocation(addr2, iv2, IndexState.LIVE);
    ri.add(location3, RACKS[1]);
    assertEquals(2, ri.getIndexes().size());
  }

  public void testRemove() {
    ri.add(location, RACKS[0]);
    InetSocketAddress addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
    IndexLocation location2 = new IndexLocation(addr2, iv, IndexState.LIVE);
    ri.add(location2, RACKS[1]);
    IndexVersion iv2 = iv.nextVersion();
    IndexLocation location3 = new IndexLocation(addr2, iv2, IndexState.LIVE);
    ri.add(location3, RACKS[1]);
    assertEquals(2, ri.getIndexes().size());

    ri.remove(location2, RACKS[1]);
    assertEquals(2, ri.getIndexes().size());
    assertTrue(ri.getReplicaAddresses(iv).contains(addr));
    assertEquals(1, ri.getReplicaAddresses(iv).size());
    assertTrue(!ri.getReplicaAddresses(iv).contains(addr2));
    assertTrue(!ri.getReplicaRacks(iv).contains(RACKS[1]));

    ri.remove(location, RACKS[0]);
    assertEquals(1, ri.getIndexes().size());
    assertNull(ri.getReplicaAddresses(iv));
    assertNull(ri.getReplicaRacks(iv));

  }

  public void testGetReplicaAddresses() {
    ri.add(location, RACKS[0]);
    assertTrue(ri.getReplicaAddresses(iv).contains(addr));

    InetSocketAddress addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
    IndexLocation location2 = new IndexLocation(addr2, iv, IndexState.LIVE);
    ri.add(location2, RACKS[1]);
    assertEquals(1, ri.getIndexes().size());
    assertTrue(ri.getReplicaAddresses(iv).contains(addr));
    assertTrue(ri.getReplicaAddresses(iv).contains(addr2));

    IndexVersion iv2 = iv.nextVersion();
    IndexLocation location3 = new IndexLocation(addr2, iv2, IndexState.LIVE);
    ri.add(location3, RACKS[1]);
    assertTrue(!ri.getReplicaAddresses(iv2).contains(addr));
    assertTrue(ri.getReplicaAddresses(iv2).contains(addr2));
  }

  public void testGetReplicaRacks() {
    ri.add(location, RACKS[0]);
    assertTrue(ri.getReplicaRacks(iv).contains(RACKS[0]));

    InetSocketAddress addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
    IndexLocation location2 = new IndexLocation(addr2, iv, IndexState.LIVE);
    ri.add(location2, RACKS[1]);
    assertTrue(ri.getReplicaRacks(iv).contains(RACKS[1]));

    IndexVersion iv2 = iv.nextVersion();
    IndexLocation location3 = new IndexLocation(addr2, iv2, IndexState.LIVE);
    ri.add(location3, RACKS[1]);
    assertTrue(!ri.getReplicaRacks(iv2).contains(RACKS[0]));
    assertTrue(ri.getReplicaRacks(iv2).contains(RACKS[1]));
  }

}
