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
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.node.IQuery;

import org.apache.log4j.Logger;

public class DefaultNodeSelectionPolicy implements INodeSelectionPolicy {

  private final static Logger LOG = Logger.getLogger(DefaultNodeSelectionPolicy.class);

  private Map<String, List<Map<String, List<String>>>> _nodeShardMap;
  private volatile int _pos = 0;

  public Map<String, List<String>> getNodeShardsMap(final IQuery query, final String[] indexNames) {
    if (_nodeShardMap == null) {
      throw new IllegalStateException("no index deployed yet, try later again...");
    }
    final HashMap<String, List<String>> resultMap = new HashMap<String, List<String>>();

    for (final String indexName : indexNames) {
      final List<Map<String, List<String>>> listWithNode2ShardMap = _nodeShardMap.get(indexName);
      if (listWithNode2ShardMap == null) {
        LOG.warn("no index with name '" + indexName + "' deployed yet, try later again...");
        continue;
      }
      if (_pos >= listWithNode2ShardMap.size()) {
        _pos = 0;
      }

      final Map<String, List<String>> oneOptionForIndex = listWithNode2ShardMap.get(_pos);
      final Set<String> nodes = oneOptionForIndex.keySet();
      for (final String node : nodes) {
        List<String> shardList = resultMap.get(node);
        if (shardList == null) {
          shardList = new ArrayList<String>();
          resultMap.put(node, shardList);
        }
        shardList.addAll(oneOptionForIndex.get(node));
      }
    }
    _pos++;
    return resultMap;
  }

  public void setShardsAndNodes(final Map<String, List<String>> indexToShards,
      final Map<String, List<String>> shardsToNode) {
    _nodeShardMap = computeMap(indexToShards, shardsToNode);
    if (LOG.isDebugEnabled()) {
      LOG.debug("computed shard map: " + _nodeShardMap);
    }
  }

  private Map<String, List<Map<String, List<String>>>> computeMap(final Map<String, List<String>> indexToShards,
      final Map<String, List<String>> shardsToNode) {
    if (indexToShards.size() == 0 || shardsToNode.size() == 0) {
      throw new IllegalArgumentException("IndexToShards or shardsToNode can't be empty.");
    }
    // a list of node to shards for each index..
    final Map<String, List<Map<String, List<String>>>> result = new HashMap<String, List<Map<String, List<String>>>>();
    // all indexes we known
    final Set<String> indexNames = indexToShards.keySet();

    // we want to create as many nodeToShard lists as we have nodes
    // serving a given shard.
    for (final String indexName : indexNames) {
      int pos = 0;
      while (true) {
        // the shards we need to server for a given index.
        final List<String> requiredShards = indexToShards.get(indexName);

        if (requiredShards.size() == 0) {
          // in case we have a corruped index with 0 shards..
          break;
        }

        // the nodes to shard map, where we collect the different
        // shards a node has to search in..
        final HashMap<String, List<String>> nodeToShardMap = new HashMap<String, List<String>>();
        boolean newSet = true;
        for (final String shard : requiredShards) {
          // now we pic one node base on our position pos. Pos will
          // be incremented with each while loop.
          final List<String> nodes = shardsToNode.get(shard);
          if (pos >= nodes.size()) {
            newSet = false;
            break;
          }
          final String node = nodes.get(pos);
          // add the shard to the list of shards the node have to
          // search in
          List<String> nodeShards = nodeToShardMap.get(node);
          if (nodeShards == null) {
            nodeShards = new ArrayList<String>();
            nodeToShardMap.put(node, nodeShards);
          }
          nodeShards.add(shard);
        }
        if (newSet) {
          // add the new generated nodeToShard map to all our maps..
          List<Map<String, List<String>>> arrayList = result.get(indexName);
          if (arrayList == null) {
            arrayList = new ArrayList<Map<String, List<String>>>();
            result.put(indexName, arrayList);
          }
          // this should be repeated until we have as much as replica
          // we have..
          arrayList.add(nodeToShardMap);
          pos++;
        } else {
          break;
        }
      }
    }
    return result;
  }

}
