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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.katta.util.CircularList;
import net.sf.katta.util.One2ManyListMap;

public class DefaultNodeSelectionPolicy implements INodeSelectionPolicy {

  private Map<String, CircularList<String>> _shardsToNodeMap = new ConcurrentHashMap<String, CircularList<String>>();

  public void update(String shard, Collection<String> nodes) {
    _shardsToNodeMap.put(shard, new CircularList<String>(nodes));
  }

  @Override
  public Collection<String> getShardNodes(String shard) {
    CircularList<String> nodeList = _shardsToNodeMap.get(shard);
    if (nodeList == null) {
      return Collections.EMPTY_LIST;
    }
    return nodeList.asList();
  }

  public List<String> remove(String shard) {
    CircularList<String> nodes = _shardsToNodeMap.remove(shard);
    if (nodes == null) {
      return Collections.emptyList();
    }
    return nodes.asList();
  }

  public void removeNode(String node) {
    Set<String> shards = _shardsToNodeMap.keySet();
    for (String shard : shards) {
      CircularList<String> nodes = _shardsToNodeMap.get(shard);
      synchronized (nodes) {
        nodes.remove(node);
      }
    }
  }

  public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
    One2ManyListMap<String, String> node2ShardsMap = new One2ManyListMap<String, String>();
    for (String shard : shards) {
      CircularList<String> nodeList = _shardsToNodeMap.get(shard);
      if (nodeList == null || nodeList.isEmpty()) {
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DefaultNodeSelectionPolicy: ");
    String sep = "";
    for (Map.Entry<String, CircularList<String>> e : _shardsToNodeMap.entrySet()) {
      builder.append(sep);
      builder.append(e.getKey());
      builder.append(" --> ");
      builder.append(e.getValue());
      sep = " ";
    }
    return builder.toString();
  }

}
