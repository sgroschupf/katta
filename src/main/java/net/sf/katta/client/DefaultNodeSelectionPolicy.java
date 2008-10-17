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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.katta.util.CircularList;
import net.sf.katta.util.One2ManyListMap;

public class DefaultNodeSelectionPolicy implements INodeSelectionPolicy {

  private Map<String, CircularList<String>> _shardsToNodeMap = new ConcurrentHashMap<String, CircularList<String>>();

  // public void update(Map<String, List<String>> shardsToNode) {
  // Map<String, CircularList<String>> newShard2NodesMap = new HashMap<String,
  // CircularList<String>>(shardsToNode.size());
  // Set<String> shards = shardsToNode.keySet();
  // for (String shard : shards) {
  // newShard2NodesMap.put(shard, new
  // CircularList<String>(shardsToNode.get(shard)));
  // }
  // _shardsToNodeMap = newShard2NodesMap;
  // }

  public void update(String shard, List<String> nodes) {
    _shardsToNodeMap.put(shard, new CircularList<String>(nodes));
  }

  public List<String> remove(String shard) {
    CircularList<String> nodes = _shardsToNodeMap.remove(shard);
    if (nodes == null) {
      return Collections.EMPTY_LIST;
    }
    return nodes.asList();
  }

  public void removeNode(String node) {
    Set<String> shards = _shardsToNodeMap.keySet();
    for (String shard : shards) {
      CircularList<String> nodes = _shardsToNodeMap.get(shard);
      nodes.remove(node);
    }
  }

  public Map<String, List<String>> createNode2ShardsMap(List<String> shards) throws ShardAccessException {
    One2ManyListMap<String, String> node2ShardsMap = new One2ManyListMap<String, String>();
    for (String shard : shards) {
      CircularList<String> nodeList = _shardsToNodeMap.get(shard);
      if (nodeList.isEmpty()) {
        throw new ShardAccessException(shard);
      }
      String node;
      synchronized (nodeList) {
        node = nodeList.getNext();
      }
      node2ShardsMap.add(node, shard);
    }
    return node2ShardsMap.asMap();
  }

}
