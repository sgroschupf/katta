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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import net.sf.katta.node.Query;

public class DefaultNodeSelectionPolicyTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + "#" + getName() + "() ~~~~~~~~~~~~~~~");
  }

  public void testIndexSpawnsMultipleNodes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexB", "shardB1", "shardB2");

    final Map<String, List<String>> shardsToNode = new HashMap<String, List<String>>();
    addNodes(shardsToNode, "shardB1", "node1");
    addNodes(shardsToNode, "shardB2", "node2");

    policy.setShardsAndNodes(indexToShards, shardsToNode);

    // now check results
    Map<String, List<String>> nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexB" });
    assertEquals(2, nodeShardsMap.size());

    assertEquals(2, extractFoundShards(nodeShardsMap).size());
    assertTrue(nodeShardsMap.get("node1").contains("shardB1"));
    assertTrue(nodeShardsMap.get("node2").contains("shardB2"));
  }

  public void testQueryMultipleIndexes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA1");
    addIndex(indexToShards, "indexB", "shardB1", "shardB2");

    final Map<String, List<String>> shardsToNode = new HashMap<String, List<String>>();
    addNodes(shardsToNode, "shardA1", "node1", "node2");
    addNodes(shardsToNode, "shardB1", "node1");
    addNodes(shardsToNode, "shardB2", "node2");

    policy.setShardsAndNodes(indexToShards, shardsToNode);

    // now check results
    Map<String, List<String>> nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA", "indexB" });
    assertEquals(2, nodeShardsMap.size());
    assertEquals(3, extractFoundShards(nodeShardsMap).size());
  }

  public void testSelection() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA", "shardB");

    final Map<String, List<String>> shardsToNode = new HashMap<String, List<String>>();
    addNodes(shardsToNode, "shardA", "node1", "node2");
    addNodes(shardsToNode, "shardB", "node1", "node2");

    policy.setShardsAndNodes(indexToShards, shardsToNode);
    Map<String, List<String>> nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(nodeShardsMap);
    nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(nodeShardsMap);
  }

  public void testSetShardsAndNodes() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA", "shardB");

    final Map<String, List<String>> shardsToNode = new HashMap<String, List<String>>();
    addNodes(shardsToNode, "shardA", "node1", "node2");
    addNodes(shardsToNode, "shardB", "node1", "node2");

    policy.setShardsAndNodes(indexToShards, shardsToNode);
    Map<String, List<String>> nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(nodeShardsMap);
    nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA" });
    assertEquals(1, nodeShardsMap.size());

    List<String> shardList = nodeShardsMap.values().iterator().next();
    assertEquals(2, shardList.size());
    assertTrue(shardList.contains("shardA"));
    assertTrue(shardList.contains("shardB"));
    System.out.println(nodeShardsMap);
  }

  private Set<String> extractFoundShards(Map<String, List<String>> nodeShardsMap) {
    Set<String> foundShards = new HashSet<String>();
    for (List<String> shards : nodeShardsMap.values()) {
      foundShards.addAll(shards);
    }
    return foundShards;
  }

  private void addNodes(Map<String, List<String>> shardsToNode, String shardName, String... nodes) {
    final List<String> nodeList = new ArrayList<String>();
    for (String node : nodes) {
      nodeList.add(node);
    }
    shardsToNode.put(shardName, nodeList);
  }

  private void addIndex(final Map<String, List<String>> indexToShards, String indexName, String... shards) {
    final List<String> shardList = new ArrayList<String>();
    for (String shard : shards) {
      shardList.add(shard);
    }
    indexToShards.put(indexName, shardList);
  }

  public void testManyShards() throws Exception {
    final DefaultNodeSelectionPolicy policy = new DefaultNodeSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    addIndex(indexToShards, "indexA", "shardA1", "shardA2", "shardA3");
    addIndex(indexToShards, "indexB", "shardB1", "shardB2", "shardB3");
    addIndex(indexToShards, "indexC", "shardC1", "shardC2", "shardC3", "shardC4");

    final Map<String, List<String>> shardsToNode = new HashMap<String, List<String>>();
    addNodes(shardsToNode, "shardA1", "node1", "node2", "node3");
    addNodes(shardsToNode, "shardA2", "node1", "node2", "node4");
    addNodes(shardsToNode, "shardA3", "node1", "node2", "node5");
    addNodes(shardsToNode, "shardB1", "node3", "node4", "node5");
    addNodes(shardsToNode, "shardB2", "node2", "node4", "node5");
    addNodes(shardsToNode, "shardB3", "node1", "node2");
    addNodes(shardsToNode, "shardC1", "node2", "node3", "node5");
    addNodes(shardsToNode, "shardC2", "node1", "node2", "node4");
    addNodes(shardsToNode, "shardC3", "node2", "node4", "node5");
    addNodes(shardsToNode, "shardC4", "node1", "node3", "node4");

    policy.setShardsAndNodes(indexToShards, shardsToNode);
    Map<String, List<String>> nodeShardsMap = policy.getNodeShardsMap(new Query(), new String[] { "indexA", "indexB",
        "indexC" });
    assertEquals(10, extractFoundShards(nodeShardsMap).size());
  }

}
