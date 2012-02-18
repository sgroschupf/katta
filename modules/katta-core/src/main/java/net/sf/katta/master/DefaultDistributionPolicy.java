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
package net.sf.katta.master;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.util.CircularList;

import org.apache.log4j.Logger;

/**
 * 
 * Simple deploy policy which distributes the shards in round robin style.<b>
 * Following features are supported:<br>
 * - initial shard distribution<br>
 * - shard distribution when under replicated<br>
 * - shard removal when over-replicated <br>
 * 
 * <p>
 * Missing feature:<br>
 * - shard/node rebalancing<br>
 * 
 * TODO jz: node load rebalancing
 */
public class DefaultDistributionPolicy implements IDeployPolicy {

  private final static Logger LOG = Logger.getLogger(DefaultDistributionPolicy.class);

  public Map<String, List<String>> createDistributionPlan(final Map<String, List<String>> currentShard2NodesMap,
          final Map<String, List<String>> currentNode2ShardsMap, List<String> aliveNodes, final int replicationLevel) {
    if (aliveNodes.size() == 0) {
      throw new IllegalArgumentException("no alive nodes to distribute to");
    }

    Set<String> shards = currentShard2NodesMap.keySet();
    for (String shard : shards) {
      Set<String> assignedNodes = new HashSet<String>(replicationLevel);
      int neededDeployments = replicationLevel - countValues(currentShard2NodesMap, shard);
      assignedNodes.addAll(currentShard2NodesMap.get(shard));

      // now assign new nodes based on round robin algorithm
      sortAfterFreeCapacity(aliveNodes, currentNode2ShardsMap);
      CircularList<String> roundRobinNodes = new CircularList<String>(aliveNodes);
      neededDeployments = chooseNewNodes(currentNode2ShardsMap, roundRobinNodes, shard, assignedNodes,
              neededDeployments);

      if (neededDeployments > 0) {
        LOG.warn("cannot replicate shard '" + shard + "' " + replicationLevel + " times, cause only "
                + roundRobinNodes.size() + " nodes connected");
      } else if (neededDeployments < 0) {
        LOG.info("found shard '" + shard + "' over-replicated");
        // TODO jz: maybe we should add a configurable threshold tha e.g. 10%
        // over replication is ok ?
        removeOverreplicatedShards(currentShard2NodesMap, currentNode2ShardsMap, shard, neededDeployments);
      }
    }
    return currentNode2ShardsMap;
  }

  private void sortAfterFreeCapacity(List<String> aliveNodes, final Map<String, List<String>> node2ShardsMap) {
    Collections.sort(aliveNodes, new Comparator<String>() {
      @Override
      public int compare(String node1, String node2) {
        int size1 = node2ShardsMap.get(node1).size();
        int size2 = node2ShardsMap.get(node2).size();
        return (size1 < size2 ? -1 : (size1 == size2 ? 0 : 1));
      }
    });
  }

  private int chooseNewNodes(final Map<String, List<String>> currentNode2ShardsMap,
          CircularList<String> roundRobinNodes, String shard, Set<String> assignedNodes, int neededDeployments) {
    String tailNode = roundRobinNodes.getTail();
    String currentNode = null;
    while (neededDeployments > 0 && !tailNode.equals(currentNode)) {
      currentNode = roundRobinNodes.getNext();
      if (!assignedNodes.contains(currentNode)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("assign " + shard + " to " + currentNode);
        }
        currentNode2ShardsMap.get(currentNode).add(shard);
        assignedNodes.add(currentNode);
        neededDeployments--;
      }
    }
    return neededDeployments;
  }

  private void removeOverreplicatedShards(final Map<String, List<String>> currentShard2NodesMap,
          final Map<String, List<String>> currentNode2ShardsMap, String shard, int neededDeployments) {
    while (neededDeployments < 0) {
      int maxShardServingCount = 0;
      String maxShardServingNode = null;
      List<String> nodeNames = currentShard2NodesMap.get(shard);
      for (String node : nodeNames) {
        int shardCount = countValues(currentNode2ShardsMap, node);
        if (shardCount > maxShardServingCount) {
          maxShardServingCount = shardCount;
          maxShardServingNode = node;
        }
      }
      currentNode2ShardsMap.get(maxShardServingNode).remove(shard);
      neededDeployments++;
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
