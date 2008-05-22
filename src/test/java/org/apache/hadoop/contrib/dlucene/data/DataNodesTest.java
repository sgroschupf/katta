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

import org.apache.hadoop.contrib.dlucene.AbstractIndexes;
import org.apache.hadoop.contrib.dlucene.IndexLocation;

public class DataNodesTest extends AbstractIndexes {

  private DataNodes n = null;

  protected void setUp() throws Exception {
    super.setUp();
    n = new DataNodes();
  }

  /**
   * Test DataNode.getIndex() for null argument.
   */
  public void testGetIndexNull() {
    try {
      n.getIndexes(null);
      fail("Should throw an exception");
    } catch (Exception e) {
      //
    }
  }

  /**
   * Test DataNode.getLastHeartBeat() for null argument.
   */
  public void testGetLastHeartBeatNull() {
    try {
      n.getLastHeartBeat(null);
      fail("Should throw an exception");
    } catch (Exception e) {
      //
    }
  }

  /**
   * Test DataNode.getStatus() for null argument.
   */
  public void testGetStatusNull() {
    try {
      n.getStatus(null);
      fail("Should throw an exception");
    } catch (Exception e) {
      //
    }
  }

  /**
   * Test DataNode.getDataNodes().
   */
  public void testGetDataNodes() {
    n.add(getStatus()[0], getSetup()[0]);
    n.add(getStatus()[1], empty);
    Set<InetSocketAddress> nodeAddrs = n.getDataNodes();
    assertEquals(2, nodeAddrs.size());
    assertTrue(containsAddress(nodeAddrs, getStatus()[0]));
    assertTrue(containsAddress(nodeAddrs, getStatus()[1]));
  }

  /**
   * Test DataNode.getStatus().
   */
  public void testGetStatus() {
    n.add(getStatus()[0], getSetup()[0]);
    assertNull(n.getStatus(getStatus()[3].getAddress()));
    assertTrue(n.getStatus(getStatus()[0].getAddress()) != null);
    assertEquals(getStatus()[0], n.getStatus(getStatus()[0].getAddress()));
  }

  /**
   * Common setup.
   */
  private void setupOne() {
    for (int i = 0; i <= 2; i++) {
      n.add(getStatus()[i], getSetup()[i]);
    }
  }

  /**
   * Test DataNode.getIndexes().
   */
  public void testGetIndexes() {
    setupOne();
    assertNull(toSet(n.getIndexes(getStatus()[3].getAddress())));

    Set<IndexLocation> i = toSet(n.getIndexes(getStatus()[0].getAddress()));
    assertEquals(2, i.size());
    assertTrue(i.contains(getSetup()[0][0]));
    assertTrue(i.contains(getSetup()[0][1]));
    Set<IndexLocation> j = toSet(n.getIndexes(getStatus()[1].getAddress()));
    assertEquals(1, j.size());
    assertTrue(j.contains(getSetup()[1][0]));
  }

  /**
   * Test DataNode.getLastHeartBeat().
   */
  public void testGetLastHeartBeat() {
    setupOne();
    assertEquals(0, n.getLastHeartBeat(getStatus()[3].getAddress()));
    long lastheartbeat = n.getLastHeartBeat(getStatus()[0].getAddress());
    assertTrue(System.currentTimeMillis() >= lastheartbeat);
  }

  /**
   * Test DataNode.getRemove().
   */
  public void testRemove() {
    setupOne();
    Set<IndexLocation> i = toSet(n.getIndexes(getStatus()[0].getAddress()));
    assertEquals(2, i.size());
    n.remove(getStatus()[0].getAddress());
    Set<IndexLocation> j = toSet(n.getIndexes(getStatus()[0].getAddress()));
    assertNull(j);
    Set<InetSocketAddress> nodes = n.getDataNodes();
    assertTrue(!nodes.contains(getStatus()[0].getAddress()));
  }
}
