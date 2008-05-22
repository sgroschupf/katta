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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.AbstractIndexes;
import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.network.Network;

public class NameNodeDataTest extends AbstractIndexes {

  private NameNodeData n = null;

  protected void setUp() throws Exception {
    super.setUp();
    n = new NameNodeData(conf);
  }

  public void testNullArguments() throws Exception {
    try {
      n.add(null, null);
      fail("addDatanode() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void setupOne() {
    n.add(getStatus()[0], getSetup()[0]);
    n.add(getStatus()[1], empty);
    n.add(getStatus()[2], empty);
    n.add(getStatus()[3], empty);
  }

  public void testGetSearchableIndexes() {
    Set<IndexLocation> i = toSet(n.getSearchableIndexes());
    assertEquals(0, i.size());
    n.add(getStatus()[0], getSetup()[0]);
    assertEquals(IndexState.REPLICATING, getSetup()[1][0].getState());
    n.add(getStatus()[1], getSetup()[1]);
    i = toSet(n.getSearchableIndexes());
    // we should not get back the replicating index
    assertEquals(2, i.size());
    assertTrue(i.contains(getSetup()[0][0]));
    assertTrue(i.contains(getSetup()[0][1]));
  }

  private static boolean matches(DataNodeStatus dns, String name) {
    return name.equals(Network.convertInetSocketAddress(dns.getAddress()));
  }

  public void testGetRandomDataNode() {
    setupOne();
    String name = n.getRandomDataNode();
    assertTrue(matches(getStatus()[0], name) || matches(getStatus()[1], name)
        || matches(getStatus()[2], name) || matches(getStatus()[3], name));
  }

  public void testAddDataNode() {
    IndexVersion version = new IndexVersion(NN_INDEX_ONE);
    IndexLocation ilA = new IndexLocation(getStatus()[0].getAddress(), version,
        IndexState.LIVE);
    IndexLocation ilB = new IndexLocation(getStatus()[0].getAddress(), version
        .nextVersion(), IndexState.LIVE);
    IndexLocation[] s1 = { ilA, ilB };
    n.add(getStatus()[0], s1);

    assertTrue(toSet(n.getSearchableIndexes()).contains(ilA));
    assertTrue(toSet(n.getSearchableIndexes()).contains(ilB));

    IndexLocation[] i = { ilA, ilB };
    n.add(getStatus()[0], i);
    assertTrue(n.getDataNodes().contains(getStatus()[0].getAddress()));
  }

  public void testCreateReplicationPlan() throws Exception {
    setupOne();
    MakeReplicationPlan rp = new MakeReplicationPlan(n);
    Map<InetSocketAddress, IndexLocation[]> plan = rp.createReplicationPlan();
    for(Map.Entry<InetSocketAddress, IndexLocation[]> entry : plan.entrySet()) {
      Set<IndexLocation> replicationPlan = toSet(entry.getValue());
      assertTrue(replicationPlan != null);
      assertTrue(replicationPlan.contains(getSetup()[0][0])
          || replicationPlan.contains(getSetup()[0][1]));
    }
  }

  public void testToString() {
    assertTrue(n.toString() != null);
  }

  public void testDoFailureDetect() throws Exception {
    setupOne();
    System.out.println("testDoFailureDetect()");
    for (IndexLocation il : n.getSearchableIndexes()) {
      System.out.println(il.toString());
    }

    // check we have no failures
    Set<InetSocketAddress> fails = n.doFailureDetect();
    assertTrue(fails.size() == 0);

    // wait for timeout
    long time = System.currentTimeMillis();
    Long failureInterval = Constants.HEARTBEAT_INTERVAL_VALUE * 3000;
    while (System.currentTimeMillis() < time + failureInterval) {
      // do nothing
    }

    // check all nodes have failed
    fails = n.doFailureDetect();
    assertTrue(fails.size() == 4);
  }

  public void testRemoveDataNode() {
    n.add(getStatus()[0], getSetup()[0]);
    assertTrue(n.getDataNodes().contains(getStatus()[0].getAddress()));
    Set<InetSocketAddress> failures = new HashSet<InetSocketAddress>();
    failures.add(getStatus()[0].getAddress());
    for (InetSocketAddress addr : failures) {
      n.remove(addr);
    }
    assertEquals(0, n.getDataNodes().size());
  }
}
