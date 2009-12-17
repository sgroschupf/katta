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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.ShardDeployOperation;
import net.sf.katta.protocol.operation.node.ShardUndeployInstruction;
import net.sf.katta.util.CollectionUtil;

import org.apache.log4j.Logger;

public abstract class AbstractIndexOperation implements LeaderOperation {

  private static final long serialVersionUID = 1L;
  protected final static Logger LOG = Logger.getLogger(AbstractIndexOperation.class);

  protected List<OperationId> distributeIndexShards(LeaderContext context, final IndexMetaData indexMD,
          Collection<String> liveNodes, boolean freshIndex) {
    InteractionProtocol protocol = context.getProtocol();
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
    cloneMap(currentShard2NodesMap);
    Map<String, List<String>> currentNode2ShardsMap = getCurrentNode2ShardMap(liveNodes, currentShard2NodesMap);

    final Map<String, List<String>> newNode2ShardMap = context.getDeployPolicy().createDistributionPlan(
            currentShard2NodesMap, cloneMap(currentNode2ShardsMap), new ArrayList<String>(liveNodes),
            indexMD.getReplicationLevel());

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

  private Map<String, List<String>> cloneMap(Map<String, List<String>> currentShard2NodesMap) {
    // return currentShard2NodesMap;
    Set<Entry<String, List<String>>> entries = currentShard2NodesMap.entrySet();
    HashMap<String, List<String>> clonedMap = new HashMap<String, List<String>>();
    for (Entry<String, List<String>> e : entries) {
      clonedMap.put(e.getKey(), new ArrayList<String>(e.getValue()));
    }
    System.out.println(currentShard2NodesMap);
    System.out.println(clonedMap);
    return clonedMap;
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

  protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol, String indexName) {
    List<String> liveNodes = protocol.getNodes();
    ReplicationReport replicationReport = getReplicationReport(protocol, indexName);
    if (replicationReport.isBalanced()) {
      return false;
    }
    if (replicationReport.isUnderreplicated()
            && liveNodes.size() <= replicationReport.getMinimalShardReplicationCount()) {
      return false;
    }
    return true;
  }

  private ReplicationReport getReplicationReport(InteractionProtocol protocol, String indexName) {
    final IndexMetaData indexMD = protocol.getIndexMD(indexName);
    int desiredReplicationCount = indexMD.getReplicationLevel();
    int minimalShardReplicationCount = indexMD.getReplicationLevel();
    int maximaShardReplicationCount = 0;

    final Set<Shard> shards = indexMD.getShards();
    for (final Shard shard : shards) {
      final int servingNodesCount = protocol.getShardNodes(shard.getName()).size();
      if (servingNodesCount < minimalShardReplicationCount) {
        minimalShardReplicationCount = servingNodesCount;
      }
      if (servingNodesCount > maximaShardReplicationCount) {
        maximaShardReplicationCount = servingNodesCount;
      }
    }
    return new ReplicationReport(desiredReplicationCount, minimalShardReplicationCount, maximaShardReplicationCount);
  }

  protected Set<String> getUnderreplicatedIndexes(final InteractionProtocol protocol, int nodeCount) {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : protocol.getIndices()) {
      ReplicationReport replicationReport = getReplicationReport(protocol, index);
      if (replicationReport.isUnderreplicated() && nodeCount <= replicationReport.getMinimalShardReplicationCount()) {
        underreplicatedIndexes.add(index);
      }
    }
    return underreplicatedIndexes;
  }

  protected Set<String> getOverreplicatedIndexes(final InteractionProtocol protocol) {
    final Set<String> overreplicatedIndexes = new HashSet<String>();
    for (final String index : protocol.getIndices()) {
      ReplicationReport replicationReport = getReplicationReport(protocol, index);
      if (replicationReport.isOverreplicated()) {
        overreplicatedIndexes.add(index);
      }
    }
    return overreplicatedIndexes;
  }

  static class ReplicationReport {

    private final int _desiredReplicationCount;
    private final int _minimalShardReplicationCount;
    private final int _maximalShardReplicationCount;

    public ReplicationReport(int desiredReplicationCount, int minimalShardReplicationCount,
            int maximalShardReplicationCount) {
      _desiredReplicationCount = desiredReplicationCount;
      _minimalShardReplicationCount = minimalShardReplicationCount;
      _maximalShardReplicationCount = maximalShardReplicationCount;
    }

    public int getDesiredReplicationCount() {
      return _desiredReplicationCount;
    }

    public int getMinimalShardReplicationCount() {
      return _minimalShardReplicationCount;
    }

    public int getMaximalShardReplicationCount() {
      return _maximalShardReplicationCount;
    }

    public boolean isUnderreplicated() {
      return getMinimalShardReplicationCount() < getDesiredReplicationCount();
    }

    public boolean isOverreplicated() {
      return getMaximalShardReplicationCount() > getDesiredReplicationCount();
    }

    public boolean isBalanced() {
      return !isUnderreplicated() && !isOverreplicated();
    }

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
