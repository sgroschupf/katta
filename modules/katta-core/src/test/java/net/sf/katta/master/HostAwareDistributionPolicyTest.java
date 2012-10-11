/**
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class HostAwareDistributionPolicyTest {

  private HostAwareDistributionPolicy _distributionPolicy;
  Map<String, List<String>> _currentShard2NodesMap;
  Map<String, List<String>> _currentNode2ShardsMap;

  @Before
  public void setUp() {
    _distributionPolicy = new HostAwareDistributionPolicy();
    _currentShard2NodesMap = new HashMap<String, List<String>>();
    _currentNode2ShardsMap = new HashMap<String, List<String>>();
  }

  @Test
  public void testInitialDistribution() throws Exception {
    int replicationLevel = 3;
    List<String> nodes = createNodes("node1:20001", "node1:20002", "node2:20001", "node2:20002", "node3:20001", "node3:20002");
    Set<String> shards = createShards("shard1", "shard2", "shard3", "shard4");
    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), node2ShardsMap.size());
    assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);

    assertTrue( Collections.disjoint(node2ShardsMap.get("node1:20001"), node2ShardsMap.get("node1:20002")));
    assertTrue( Collections.disjoint(node2ShardsMap.get("node2:20001"), node2ShardsMap.get("node2:20002")));
    assertTrue( Collections.disjoint(node2ShardsMap.get("node3:20001"), node2ShardsMap.get("node3:20002")));
  }

  @Test
  public void testThreeRoundsDistribution() throws Exception {
    int replicationLevel = 1;
    List<String> nodes = createNodes("node1:20001", "node1:20002", "node2:20001", "node2:20002", "node3:20001", "node3:20002");
    Set<String> shards = createShards("shard1", "shard2", "shard3", "shard4");
    Map<String, List<String>> firstNode2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), firstNode2ShardsMap.size());

    updateShardToNodesMap(firstNode2ShardsMap);
    replicationLevel = 2;
    Map<String, List<String>> secondNode2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, firstNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), secondNode2ShardsMap.size());
    assertSufficientDistribution(replicationLevel, nodes, shards, secondNode2ShardsMap);

    updateShardToNodesMap(secondNode2ShardsMap);
    replicationLevel = 3;
    Map<String, List<String>> thirdNode2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, secondNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), thirdNode2ShardsMap.size());
    assertSufficientDistribution(replicationLevel, nodes, shards, thirdNode2ShardsMap);

    assertTrue( Collections.disjoint(thirdNode2ShardsMap.get("node1:20001"), thirdNode2ShardsMap.get("node1:20002")));
    assertTrue( Collections.disjoint(thirdNode2ShardsMap.get("node2:20001"), thirdNode2ShardsMap.get("node2:20002")));
    assertTrue( Collections.disjoint(thirdNode2ShardsMap.get("node3:20001"), thirdNode2ShardsMap.get("node3:20002")));
  }

  @Test
  public void testTypicalDeployedDistribution() throws Exception {
    int replicationLevel = 2;
    List<String> nodes = createNodesByHosts(4, 6);
    createNumShards(46);
    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), node2ShardsMap.size());
    assertDisjointNodesOnEachHost(4, 6, node2ShardsMap);
  }


  @Test
  public void testEqualDistributionOnMultipleSequentialDeploys() throws Exception {
    int replicationLevel = 1;
    List<String> nodes = createNodes("node1:20000", "node2:20000", "node3:20000", "node4:20000");
    createShards("shard1", "shard2");
    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    System.out.println(node2ShardsMap);

    _currentShard2NodesMap.clear();
    createShards("shard3", "shard4");
    node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap, _currentNode2ShardsMap, nodes,
        replicationLevel);
    for (String node : node2ShardsMap.keySet()) {
      assertEquals("shards are not equally distributed: " + node2ShardsMap, 1, node2ShardsMap.get(node).size());
    }
    System.out.println(node2ShardsMap);
  }

  @Test
  public void testInitialDistribution_TooLessNodes() throws Exception {
    List<String> nodes = createNodes("node1:20000");
    Set<String> shards = createShards("shard1", "shard2");
    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, 3);
    assertEquals(nodes.size(), node2ShardsMap.size());
    assertEquals(shards.size(), node2ShardsMap.get("node1:20000").size());
  }

  @Test
  public void testUnderReplicatedDistribution() throws Exception {
    int replicationLevel = 3;
    List<String> nodes = createNodes("node1:20000", "node2:20000", "node3:20000");
    Set<String> shards = createShards("shard1", "shard2", "shard3");
    addMapping("shard1", "node1:20000", "node2:20000", "node3:20000");
    addMapping("shard2", "node1:20000");

    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), node2ShardsMap.size());
    assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
  }

  @Test
  public void testOverReplicatedDistribution() throws Exception {
    int replicationLevel = 2;
    List<String> nodes = createNodes("node1:20000", "node2:20000", "node3:20000", "node4:20000");
    Set<String> shards = createShards("shard1", "shard2");
    addMapping("shard1", "node1:20000", "node2:20000", "node3:20000", "node4:20000");
    addMapping("shard2", "node1:20000", "node2:20000");

    Map<String, List<String>> node2ShardsMap = _distributionPolicy.createDistributionPlan(_currentShard2NodesMap,
        _currentNode2ShardsMap, nodes, replicationLevel);
    assertEquals(nodes.size(), node2ShardsMap.size());
    assertSufficientDistribution(replicationLevel, nodes, shards, node2ShardsMap);
  }

  private void assertSufficientDistribution(int replicationLevel, List<String> nodes, Set<String> shards,
                        Map<String, List<String>> node2ShardsMap) {
    int deployedShardCount = 0;
    for (String node : nodes) {
      deployedShardCount += node2ShardsMap.get(node).size();
      assertTrue(node2ShardsMap.get(node).size() >= 1);
      assertTrue(node2ShardsMap.get(node).size() <= replicationLevel);
    }
    assertEquals(shards.size() * replicationLevel, deployedShardCount);
  }

  private void assertDisjointNodesOnEachHost(int numNodes, int numHosts, Map<String, List<String>> node2ShardsMap) {
    for (int h = 1; h < numHosts; h++) {
      for (int n = 1; n < numNodes; n++) {
        String node1 = createNodeString(n, h);
        String node2 = createNodeString(n+1, h);
        assertTrue( Collections.disjoint( node2ShardsMap.get(node1), node2ShardsMap.get(node2)));
      }
    }
  }

  private void updateShardToNodesMap(Map<String, List<String>> node2ShardsMap) {
    for (String node : node2ShardsMap.keySet()) {
      for (String shard : node2ShardsMap.get(node)) {
        if (!_currentShard2NodesMap.containsKey(shard)) {
          _currentShard2NodesMap.put(shard, new ArrayList<String>());
        }
        if (!_currentShard2NodesMap.get(shard).contains(node)) {
          _currentShard2NodesMap.get(shard).add(node);
        }
      }
    }
  }

  private void addMapping(String shard, String... nodes) {
    List<String> shardNodes = _currentShard2NodesMap.get(shard);
    for (String node : nodes) {
      List<String> shards = _currentNode2ShardsMap.get(node);
      shards.add(shard);
      shardNodes.add(node);
    }
  }

  private List<String> createNodes(String... nodeNames) {
    List<String> nodes = new ArrayList<String>();
    for (String nodeName : nodeNames) {
      nodes.add(nodeName);
      _currentNode2ShardsMap.put(nodeName, new ArrayList<String>());
    }
    return nodes;
  }

  private Set<String> createShards(String... shardNames) {
    Set<String> shards = new HashSet<String>();
    for (String shardName : shardNames) {
      shards.add(shardName);
      if (!_currentNode2ShardsMap.containsKey(shardName)) {
        _currentShard2NodesMap.put(shardName, new ArrayList<String>());
      }
    }
    return shards;
  }

  private List<String> createNodesByHosts(int numNodes, int numHosts) {
    List<String> nodeNames = new ArrayList<String>();
    for (int n = 1; n <= numNodes; n++) {
      for (int h = 1; h <= numHosts; h++) {
        String node = createNodeString(n, h);
        nodeNames.add(node);
      }
    }
    return createNodes(nodeNames.toArray(new String[numNodes*numHosts]));
  }

  private String createNodeString(int n, int h) {
    return String.format("host%d:2000%d", h, n);
  }

  private Set<String> createNumShards(int num) {
    List<String> shardNames = new ArrayList<String>();
    for (int i = 1; i <= num; i++) {
      shardNames.add(String.format("shard%d", i));
    }
    return createShards(shardNames.toArray(new String[num]));
  }
}