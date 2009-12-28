/**
 * Copyright 2008 the original author or authors.
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
package net.sf.katta.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.AbstractTest;

import org.junit.Test;

public class DefaultNodeSelectionPolicyTest extends AbstractTest {

  @Test
  public void testIndexSpawnsMultipleNodes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexB", "shardB1", "shardB2");

    addNodes(policy, "shardB1", "node1");
    addNodes(policy, "shardB2", "node2");

    // now check results
    Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(indexToShards.get("indexB"));
    assertEquals(2, nodeShardsMap.size());

    assertEquals(2, extractFoundShards(nodeShardsMap).size());
    assertTrue(nodeShardsMap.get("node1").contains("shardB1"));
    assertTrue(nodeShardsMap.get("node2").contains("shardB2"));
  }

  @Test
  public void testQueryMultipleIndexes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA1");
    addIndex(indexToShards, "indexB", "shardB1", "shardB2");

    addNodes(policy, "shardA1", "node1", "node2");
    addNodes(policy, "shardB1", "node1");
    addNodes(policy, "shardB2", "node2");

    // now check results
    List<String> shards = new ArrayList<String>();
    shards.addAll(indexToShards.get("indexA"));
    shards.addAll(indexToShards.get("indexB"));
    Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(shards);
    assertEquals(2, nodeShardsMap.size());
    assertEquals(3, extractFoundShards(nodeShardsMap).size());
  }

  @Test
  public void testSelection() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA", "shardB");

    addNodes(policy, "shardA", "node1", "node2");
    addNodes(policy, "shardB", "node1", "node2");

    Map<String, List<String>> nodeShardsMap1 = policy.createNode2ShardsMap(indexToShards.get("indexA"));
    Map<String, List<String>> nodeShardsMap2 = policy.createNode2ShardsMap(indexToShards.get("indexA"));
    assertEquals(1, nodeShardsMap1.size());
    assertEquals(1, nodeShardsMap2.size());
    assertFalse("nodes should differ", nodeShardsMap1.keySet().equals(nodeShardsMap2.keySet()));
  }

  @Test
  public void testSetShardsAndNodes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA", "shardB");

    addNodes(policy, "shardA", "node1", "node2");
    addNodes(policy, "shardB", "node1", "node2");

    Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(indexToShards.get("indexA"));
    assertEquals(1, nodeShardsMap.size());

    List<String> shardList = nodeShardsMap.values().iterator().next();
    assertEquals(2, shardList.size());
    assertTrue(shardList.contains("shardA"));
    assertTrue(shardList.contains("shardB"));
  }

  private Set<String> extractFoundShards(Map<String, List<String>> nodeShardsMap) {
    Set<String> foundShards = new HashSet<String>();
    for (List<String> shards : nodeShardsMap.values()) {
      foundShards.addAll(shards);
    }
    return foundShards;
  }

  private void addNodes(INodeSelectionPolicy policy, String shardName, String... nodes) {
    final List<String> nodeList = new ArrayList<String>();
    for (String node : nodes) {
      nodeList.add(node);
    }
    policy.update(shardName, nodeList);
  }

  private void addIndex(final Map<String, List<String>> indexToShards, String indexName, String... shards) {
    final List<String> shardList = new ArrayList<String>();
    for (String shard : shards) {
      shardList.add(shard);
    }
    indexToShards.put(indexName, shardList);
  }

  @Test
  public void testManyShards() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA1", "shardA2", "shardA3");
    addIndex(indexToShards, "indexB", "shardB1", "shardB2", "shardB3");
    addIndex(indexToShards, "indexC", "shardC1", "shardC2", "shardC3", "shardC4");

    addNodes(policy, "shardA1", "node1", "node2", "node3");
    addNodes(policy, "shardA2", "node1", "node2", "node4");
    addNodes(policy, "shardA3", "node1", "node2", "node5");
    addNodes(policy, "shardB1", "node3", "node4", "node5");
    addNodes(policy, "shardB2", "node2", "node4", "node5");
    addNodes(policy, "shardB3", "node1", "node2");
    addNodes(policy, "shardC1", "node2", "node3", "node5");
    addNodes(policy, "shardC2", "node1", "node2", "node4");
    addNodes(policy, "shardC3", "node2", "node4", "node5");
    addNodes(policy, "shardC4", "node1", "node3", "node4");

    List<String> shards = new ArrayList<String>();
    shards.addAll(indexToShards.get("indexA"));
    shards.addAll(indexToShards.get("indexB"));
    shards.addAll(indexToShards.get("indexC"));
    Map<String, List<String>> nodeShardsMap = policy.createNode2ShardsMap(shards);
    assertEquals(10, extractFoundShards(nodeShardsMap).size());
  }

}
