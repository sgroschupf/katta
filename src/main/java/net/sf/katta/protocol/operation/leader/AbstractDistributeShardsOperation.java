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
package net.sf.katta.protocol.operation.leader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.master.IDeployPolicy;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.ShardDeployOperation;
import net.sf.katta.protocol.operation.node.ShardUndeployInstruction;
import net.sf.katta.util.CollectionUtil;

import org.apache.log4j.Logger;

public abstract class AbstractDistributeShardsOperation implements LeaderOperation {

  private static final long serialVersionUID = 1L;
  protected final static Logger LOG = Logger.getLogger(AbstractDistributeShardsOperation.class);

  protected List<OperationId> distributeIndexShards(InteractionProtocol protocol, IDeployPolicy deployPolicy,
          final IndexMetaData indexMD, Collection<String> liveNodes, boolean freshIndex) {
    Set<Shard> shards = indexMD.getShards();

    if (freshIndex) {
      // cleanup/undeploy failed shards
      for (Shard shard : shards) {
        protocol.cleanupShardData(shard.getName());
      }

      // add shards to zk
      protocol.createShardData(indexMD);
    }

    // now distribute shards
    final Map<String, List<String>> currentShard2NodesMap = protocol.getShard2NodesMap(shards);
    Map<String, List<String>> currentNode2ShardsMap = getCurrentNode2ShardMap(liveNodes, currentShard2NodesMap);

    final Map<String, List<String>> newNode2ShardMap = deployPolicy.createDistributionPlan(currentShard2NodesMap,
            currentNode2ShardsMap, new ArrayList<String>(liveNodes), indexMD.getReplicationLevel());
    currentNode2ShardsMap = getCurrentNode2ShardMap(liveNodes, currentShard2NodesMap);

    // System.out.println(distributionMap);// node to shards
    Set<String> nodes = newNode2ShardMap.keySet();
    List<OperationId> operationIds = new ArrayList<OperationId>(nodes.size());
    for (String node : nodes) {
      List<String> nodeShards = newNode2ShardMap.get(node);
      List<String> listOfAdded = CollectionUtil.getListOfAdded(currentNode2ShardsMap.get(node), nodeShards);
      if (!listOfAdded.isEmpty()) {
        ShardDeployOperation deployInstruction = new ShardDeployOperation();
        for (String shard : listOfAdded) {
          deployInstruction.addShard(shard, indexMD.getShardPath(shard));
        }
        OperationId operationId = protocol.addNodeOperation(node, deployInstruction);
        operationIds.add(operationId);
      }
      List<String> listOfRemoved = CollectionUtil.getListOfRemoved(currentNode2ShardsMap.get(node), nodeShards);
      if (!listOfRemoved.isEmpty()) {
        ShardUndeployInstruction undeployInstruction = new ShardUndeployInstruction(listOfRemoved);
        OperationId operationId = protocol.addNodeOperation(node, undeployInstruction);
        operationIds.add(operationId);
      }
    }
    return operationIds;
  }

  private Map<String, List<String>> getCurrentNode2ShardMap(Collection<String> liveNodes,
          final Map<String, List<String>> currentShard2NodesMap) {
    final Map<String, List<String>> currentNodeToShardsMap = CollectionUtil.invertListMap(currentShard2NodesMap);
    for (String node : liveNodes) {
      if (!currentNodeToShardsMap.containsKey(node)) {
        currentNodeToShardsMap.put(node, new ArrayList<String>());
      }
    }
    return currentNodeToShardsMap;
  }

  public String createShardName(String indexName, String shardPath) {
    int lastIndexOf = shardPath.lastIndexOf("/");
    if (lastIndexOf == -1) {
      lastIndexOf = 0;
    }
    String shardFolderName = shardPath.substring(lastIndexOf + 1, shardPath.length());
    if (shardFolderName.endsWith(".zip")) {
      shardFolderName = shardFolderName.substring(0, shardFolderName.length() - 4);
    }
    return indexName + "_" + shardFolderName;
  }

  class IndexInvalidException extends Exception {

    private static final long serialVersionUID = 1L;

    public IndexInvalidException(final String message) {
      super(message);
    }

    public IndexInvalidException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
