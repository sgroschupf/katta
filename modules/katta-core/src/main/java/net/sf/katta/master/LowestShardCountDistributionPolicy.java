/**
j * Copyright 2008 the original author or authors.
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * This code attempts to select the node that has the fewest shards, to deploy
 * replicate shards to, and to select the node that has the most shards to
 * remove excess replicas from.
 * 
 * @deprecated DefaultDistributionPolicy already contains the same functionality
 *             since version 6.1
 */
public class LowestShardCountDistributionPolicy implements IDeployPolicy {

  private final static Logger LOG = Logger.getLogger(LowestShardCountDistributionPolicy.class);

  Comparator<Map.Entry<String, AtomicInteger>> aliveComparator = new Comparator<Map.Entry<String, AtomicInteger>>() {

    @Override
    public int compare(Entry<String, AtomicInteger> o1, Entry<String, AtomicInteger> o2) {
      return o2.getValue().get() - o1.getValue().get();
    }

  };

  static class OrderedBucket implements Comparable<OrderedBucket> {
    final Map.Entry<String, AtomicInteger> bucket;

    public OrderedBucket(final Map.Entry<String, AtomicInteger> bucket) {
      this.bucket = bucket;
    }

    public String getKey() {
      return bucket.getKey();
    }

    public AtomicInteger getValue() {
      return bucket.getValue();
    }

    @Override
    public int compareTo(OrderedBucket o2) {
      if (o2 == this) {
        return 0;
      }
      // sort null entries to the end
      if (bucket == null) {
        return 1;
      }
      if (o2.bucket == null) {
        return -1;
      }
      int res = getValue().get() - o2.getValue().get();
      if (res != 0) {
        return res;
      }
      res = o2.getKey().compareTo(getKey());
      if (res != 0) {
        return res;
      }
      return o2.hashCode() - hashCode();
    }

    public String toString() {
      if (bucket == null) {
        return "null";
      }
      return "[ " + bucket.getKey() + '=' + bucket.getValue().get() + "] ";
    }
  }

  private TreeSet<OrderedBucket> produceOrdedNodeList(final Map<String, List<String>> currentNode2ShardsMap,
          List<String> aliveNodes) {

    HashMap<String, AtomicInteger> nodeShardCountsMap = new HashMap<String, AtomicInteger>();
    for (String node : aliveNodes) {
      nodeShardCountsMap.put(node, new AtomicInteger(0));
    }
    for (Map.Entry<String, List<String>> shardInfo : currentNode2ShardsMap.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Working on shard: %s, with list %s", shardInfo.getKey(), shardInfo.getValue()));
      }

      final List<String> shards = shardInfo.getValue();
      if (shards == null || shards.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("empty node %s", shardInfo.getKey()));
        }
        continue;
      }

      AtomicInteger b = nodeShardCountsMap.get(shardInfo.getKey());
      if (b == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Skipping node %s, not in live list, carries shards %s", shardInfo.getKey(),
                  shardInfo.getValue()));
        }
        continue; // node is not alive
      }

      b.addAndGet(shards.size());
    }
    TreeSet<OrderedBucket> sortedAliveNodes = new TreeSet<OrderedBucket>();
    for (Map.Entry<String, AtomicInteger> bucket : nodeShardCountsMap.entrySet()) {
      sortedAliveNodes.add(new OrderedBucket(bucket));
    }

    return sortedAliveNodes;
  }

  public Map<String, List<String>> createDistributionPlan(final Map<String, List<String>> currentShard2NodesMap,
          final Map<String, List<String>> currentNode2ShardsMap, List<String> aliveNodes, final int replicationLevel) {
    if (aliveNodes.size() == 0) {
      throw new IllegalArgumentException("no alive nodes to distribute to");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Live nodes are %d, %s", aliveNodes.size(), aliveNodes));
      LOG.debug(String.format("Current Shard 2 Nodes Map, %s", currentShard2NodesMap));
      LOG.debug(String.format("Current Nodes 2 Shard Map, %s", currentNode2ShardsMap));
    }

    Set<String> shards = currentShard2NodesMap.keySet();
    TreeSet<OrderedBucket> orderedNodes = produceOrdedNodeList(currentNode2ShardsMap, aliveNodes);

    for (String shard : shards) {
      Set<String> assignedNodes = new HashSet<String>(replicationLevel);
      int neededDeployments = replicationLevel - countValues(currentShard2NodesMap, shard);
      assignedNodes.addAll(currentShard2NodesMap.get(shard));

      // now assign new nodes based on round robin algorithm
      neededDeployments = chooseNewNodes(currentNode2ShardsMap, orderedNodes, shard, assignedNodes, neededDeployments);

      if (neededDeployments > 0) {
        LOG.warn("cannot replicate shard '" + shard + "' " + replicationLevel + " times, cause only "
                + orderedNodes.size() + " nodes connected");
      } else if (neededDeployments < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("found shard '" + shard + "' over-replicated");
        }
        // TODO jz: maybe we should add a configurable threshold tha e.g. 10%
        // over replication is ok ?
        removeOverreplicatedShards(currentShard2NodesMap, currentNode2ShardsMap, orderedNodes, shard, neededDeployments);
      }
    }
    return currentNode2ShardsMap;
  }

  /**
   * Place a shard into the left most element or ordered nodes that can hold
   * them. orderedNodes is modified, as is currentNode2ShardsMap, which gets the
   * additional shard added to the selected node.
   */
  private int chooseNewNodes(final Map<String, List<String>> currentNode2ShardsMap,
          TreeSet<OrderedBucket> orderedNodes, String shard, Set<String> assignedNodes, int neededDeployments) {

    String currentNode = null;
    ArrayList<OrderedBucket> toAddBack = new ArrayList<OrderedBucket>();
    try {
      for (Iterator<OrderedBucket> ordered = orderedNodes.iterator(); neededDeployments > 0 && ordered.hasNext();) {
        final OrderedBucket nodeShards = ordered.next();
        currentNode = nodeShards.getKey();

        // This is the lowest weight node, so this shard is either going here,
        // or already present
        toAddBack.add(nodeShards); // remove form the working list, to save
        // later loop work, and add it to the add
        // back list
        ordered.remove();

        final boolean nodeContainsShard = assignedNodes.contains(currentNode);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("chooseNewNodes: for shard %s, Current test node is %s, shardCount %d, %s", shard,
                  currentNode, nodeShards.getValue().get(), nodeContainsShard ? "contains shard" : "can take shard"));
        }

        if (nodeContainsShard) {
          continue;
        }
        currentNode2ShardsMap.get(currentNode).add(shard);
        assignedNodes.add(currentNode);
        nodeShards.getValue().incrementAndGet(); // Increment the count
        neededDeployments--;
      }
    } finally { // Put any nodes that were removed form the ordered list back,
      // this automatically re-orders the list as well
      orderedNodes.addAll(toAddBack);
    }
    return neededDeployments;
  }

  /**
   * This could be modified to use the orderedNodes, but it works well enough
   * for the small data set sizes
   */

  private void removeOverreplicatedShards(final Map<String, List<String>> currentShard2NodesMap,
          final Map<String, List<String>> currentNode2ShardsMap, TreeSet<OrderedBucket> orderedNodes, String shard,
          int neededDeployments) {
    Iterator<OrderedBucket> ordered = orderedNodes.descendingIterator();

    ArrayList<OrderedBucket> toAddBack = new ArrayList<OrderedBucket>();
    try {

      while (neededDeployments < 0 && ordered.hasNext()) {
        final OrderedBucket nodeShards = ordered.next();

        String maxShardServingNode = null;
        List<String> nodeNames = currentShard2NodesMap.get(shard);
        HashSet<String> nodeNameMap = new HashSet<String>(nodeNames);

        toAddBack.add(nodeShards); // remove form the working list, to save
        // later loop work, and add it to the add
        // back list
        ordered.remove();

        final String testNode = nodeShards.getKey();

        if (!nodeNameMap.contains(testNode)) {
          continue; // doesn't contain one of our items, remove from the working
          // set and try the next most loaded node
        }
        // This node contains our shard and is the most loaded (by shard count)
        if (LOG.isDebugEnabled()) {
          LOG.debug("remove " + shard + " from " + maxShardServingNode);
        }
        currentNode2ShardsMap.get(testNode).remove(shard);
        nodeShards.getValue().decrementAndGet(); // change the count
        neededDeployments++;
      }
    } finally {
      orderedNodes.addAll(toAddBack);
    }
  }

  private int countValues(Map<String, List<String>> multiMap, String key) {
    List<String> list = multiMap.get(key);
    if (list == null) {
      return 0;
    }
    return list.size();
  }

}
