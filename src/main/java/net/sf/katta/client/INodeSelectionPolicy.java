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

import java.util.List;
import java.util.Map;

import net.sf.katta.node.IQuery;

/**
 * Returns a Map with Nodes and shards within those nodes that have to be
 * searched by the client.
 * 
 * Since shards can be replicated over different nodes and nodes can be
 * distributed in different network sections (same switch or rack, same data
 * center etc.) we allow custom selection policies to implement the logic to
 * make the smartest possible choice of nodes the client has to query. The node
 * selection policy is also the place where an load balancing schema need to be
 * implemented.
 */
public interface INodeSelectionPolicy {

  /**
   * During startup or as soon the client get a notification about any change in
   * the gird the client sets the indexToShards and shcardsToNode maps. An
   * {@link INodeSelectionPolicy} should try to precompute as much as possible
   * in this method.
   * 
   * @param indexToShards
   * @param shardsToNode
   */
  public void setShardsAndNodes(Map<String, List<String>> indexToShards, Map<String, List<String>> shardsToNode);

  /**
   * Returns a map where as key the nodeName is used and as value a list shards
   * served by node we need to query. Ideally this method returns nodes with low
   * latency to the client and alternate between nodes to load balance high
   * traffic.
   * 
   * @param queries
   * @param indexNames
   * @return
   */
  public Map<String, List<String>> getNodeShardsMap(IQuery queries, String[] indexNames);

}
