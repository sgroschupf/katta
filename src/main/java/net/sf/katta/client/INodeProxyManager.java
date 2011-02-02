/**
 * Copyright 2009 the original author or authors.
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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * The interaction required between a NodeInteraction and Client.
 */
public interface INodeProxyManager {

  /**
   * Get the dynamic proxy for a node. Methods invoked on this object will be
   * sent via RPC to the server and executed there.
   * 
   * @param node
   *          The node name to look up.
   * @param establishIfNoExists
   * @return a dynamic proxy standing in for the node.
   */
  public VersionedProtocol getProxy(String node, boolean establishIfNoExists);

  /**
   * Notifies the proxy-manager that a a proxy invocation failed.
   * 
   * @param node
   *          Which node had a problem.
   * @param t
   *          The error that occurred (currently unused).
   */
  public void reportNodeCommunicationFailure(String node, Throwable t);

  /**
   * Notifies the proxy-manager that a a proxy invocation succeeded.
   * 
   * @param node
   */
  public void reportNodeCommunicationSuccess(String node);

  /**
   * After an error the NodeInteraction computes a reduced node shard map, but
   * it needs this call to use the Client's node selection policy to choose
   * which nodes to use for the retry (if there are any alternate nodes). Then
   * jobs are resubmitted for the failed shards on new nodes (see
   * INodeExecutor).
   * 
   * @param shards
   *          the mapping with the failed node removed (shards may occur
   *          multiple times).
   * @return A node to shard map with one occurrence of each shard.
   * @throws ShardAccessException
   *           if the node selection policy had an error.
   */
  public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException;

  public void shutdown();

}
