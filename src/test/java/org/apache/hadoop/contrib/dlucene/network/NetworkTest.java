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
package org.apache.hadoop.contrib.dlucene.network;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.contrib.dlucene.network.NetworkTopology.InnerNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

public class NetworkTest extends TestUtils {

  Network network = null;
  DataNodeStatus[] dnstatus = new DataNodeStatus[5];
  String firstNode = null;

  protected void setUp() throws Exception {
    super.setUp();
    network = new Network();
    int[] whichRack = { 0, 0, 1, 0, 0 };
    for (int j = 0; j < whichRack.length; j++) {
      dnstatus[j] = makeDataNodeStatus(MACHINES[j], RACKS[whichRack[j]]);
    }
    for (int i = 0; i <= 3; i++) {
      network.add(dnstatus[i]);
    }
    firstNode = Network.convertInetSocketAddress(dnstatus[0].getAddress());
  }

  public void testToNode() {
    Node node = Network.toNode(dnstatus[0]);
    network.add(node);
    assertEquals(firstNode, node.getName());
    assertEquals(RACKS[0], node.getNetworkLocation());
    assertEquals(2, node.getLevel());
  }

  public void testRemoveNode() {
    assertTrue(network.contains(dnstatus[3]));
    network.remove(dnstatus[3]);
    assertFalse(network.contains(dnstatus[3]));
    assertEquals(3, network.getNumOfLeaves());
    network.remove(dnstatus[4]);
  }

  public void testContains() {
    assertTrue(network.contains(dnstatus[0]));
    assertFalse(network.contains(dnstatus[4]));
  }

  public void testGetNode() {
    Node node = network.get(dnstatus[0]);
    assertEquals(firstNode, node.getName());
    assertEquals(RACKS[0], node.getNetworkLocation());
    assertEquals(2, node.getLevel());
  }

  public void testGetNumOfRacks() throws Exception {
    assertEquals(2, network.getNumOfRacks());
  }

  public void testGetNumOfLeaves() throws Exception {
    assertEquals(4, network.getNumOfLeaves());
  }

  public void testIsOnSameRack() {
    assertTrue(network.isOnSameRack(dnstatus[0], dnstatus[1]));
    assertTrue(network.isOnSameRack(dnstatus[0], dnstatus[3]));
    assertFalse(network.isOnSameRack(dnstatus[0], dnstatus[2]));
  }

  public void testChooseRandom() {
    Node node = network.chooseRandom("");
    assert (node.getName().equals(MACHINES[0])
        || node.getName().equals(MACHINES[1])
        || node.getName().equals(MACHINES[2]) || node.getName().equals(
        MACHINES[3]));
    Node n1 = network.chooseRandom(RACKS[0]);
    assert (n1.getName().equals(MACHINES[0])
        || n1.getName().equals(MACHINES[1]) || n1.getName().equals(MACHINES[3]));
    Node n2 = network.chooseRandom(RACKS[1]);
    assert (n2.getName().equals(MACHINES[2]));

  }

  public void testCountNumOfAvailableNodes() throws Exception {
    assertEquals(4, network.countNumOfAvailableNodes("", new ArrayList<Node>()));
    assertEquals(3, network.countNumOfAvailableNodes(RACKS[0],
        new ArrayList<Node>()));
    assertEquals(1, network.countNumOfAvailableNodes(RACKS[1],
        new ArrayList<Node>()));
  }

  public void testGetRandomNodeOne() throws Exception {
    assertTrue(network.contains(dnstatus[3]));
    int[] res1 = { 0, 1, 2, 3 };
    check(network.getRandomNode(""), dnstatus, res1);
  }

  public void testGetRandomNodeTwo() throws Exception {
    int[] res2 = { 0, 1, 3 };
    check(network.getRandomNode(RACKS[0]), dnstatus, res2);
  }

  public void testGetRandomNodeThree() throws Exception {
    int[] res3 = { 2 };
    check(network.getRandomNode(RACKS[1]), dnstatus, res3);
  }

  public void testGetRandomNodeFour() throws Exception {
    int[] res4 = { 2 };
    check(network.getRandomNode("~" + RACKS[0]), dnstatus, res4);
  }

  public void testGetRandomNodeFive() throws Exception {
    int[] res5 = { 0, 1, 3 };
    check(network.getRandomNode("~" + RACKS[1]), dnstatus, res5);
  }

  public void testGetRandomNodeSix() throws Exception {
    int[] res6 = { 0 };
    check(network.getRandomNode(Network.toLocation(dnstatus[0])), dnstatus,
        res6);
  }

  public void testStuffTwo() {
    Node scopeNode = network.getNode(NodeBase.ROOT);
    InnerNode innerNode = (InnerNode) scopeNode;
    int numOfDatanodes = innerNode.getNumOfLeaves();
    Node excludedScopeNode = null;
    if (numOfDatanodes > 0) {
      Set<String> seen = new HashSet<String>();
      for (int i = 0; i <= 10; i++) {
        Node resultNode = innerNode.getLeaf(i % numOfDatanodes,
            excludedScopeNode);
        if (resultNode != null) {
          if (!seen.contains(resultNode.getName())) {
            System.out.println("Using index " + i + " and getting back "
                + resultNode.getName());
          }
        }
      }
    }
  }
  
  public void testStuff() {
    String excludedScope = Network.toLocation(dnstatus[0]);
    Node scopeNode = network.getNode(NodeBase.ROOT);
    InnerNode innerNode = (InnerNode) scopeNode;
    int numOfDatanodes = innerNode.getNumOfLeaves();
    Node excludedScopeNode = excludedScope != null ? network
        .getNode(excludedScope) : null;
    if (numOfDatanodes > 0) {
      Set<String> seen = new HashSet<String>();
      for (int i = 0; i <= 10; i++) {
        Node resultNode = innerNode.getLeaf(i % numOfDatanodes,
            excludedScopeNode);
        if (resultNode != null) {
          if (!seen.contains(resultNode.getName())) {
            System.out.println("Using index " + i + " and getting back "
                + resultNode.getName());
          }
        }
      }
    }
  }

  public void testGetRandomNodeSeven() throws Exception {
    int[] res7 = { 1, 2, 3 };
    check(network.getRandomNode("~" + Network.toLocation(dnstatus[0])),
        dnstatus, res7);
  }

  private void check(Collection<InetSocketAddress> r, DataNodeStatus[] dns,
      int[] v) {
    for (int i = 0; i < r.size(); i++) {
      System.out.println("R " + r.toArray()[i]);
    }
    for (int i = 0; i < v.length; i++) {
      System.out.println("DNS " + dns[v[i]].getAddress());
      assertTrue(r.contains(dns[v[i]].getAddress()));
    }
    assertEquals(v.length, r.size());
  }

  public void testConvertInetSocketAddress() {
    String name = MACHINES[0] + ":" + getNextPort();
    InetSocketAddress addr = NetUtils.createSocketAddr(name);
    assertTrue(addr != null);
    String nameBack = Network.convertInetSocketAddress(addr);
    assertTrue(nameBack != null);
    assertEquals(nameBack, name);

    String brokenName = MACHINES[0] + ":";
    try {
      NetUtils.createSocketAddr(brokenName);
      fail("getInetSocketAddress() should have thrown an exception");
    } catch (Exception iae) {
      // expected
    }
  }
}
