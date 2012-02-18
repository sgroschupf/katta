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

import java.util.List;
import java.util.Map;

/**
 * Exchangeable policy for which creates an distribution plan for the shards of
 * one index.
 * 
 */
public interface IDeployPolicy {

  /**
   * Creates a distribution plan for the shards of one index. Note that the
   * index can already be deployed, in that case its more a "replication" plan.
   * 
   * @param currentShard2NodesMap
   *          all current deployments of the shards of the one index to
   *          distribute/replicate
   * @param currentNode2ShardsMap
   *          all nodes and their shards
   * @param aliveNodes
   * @param replicationLevel
   * @return
   */
  Map<String, List<String>> createDistributionPlan(Map<String, List<String>> currentShard2NodesMap,
          Map<String, List<String>> currentNode2ShardsMap, List<String> aliveNodes, int replicationLevel);

}
