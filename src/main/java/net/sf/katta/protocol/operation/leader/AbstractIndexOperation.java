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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.ReplicationReport;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexDeployError.ErrorType;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.DeployResult;
import net.sf.katta.protocol.operation.node.OperationResult;
import net.sf.katta.protocol.operation.node.ShardDeployOperation;
import net.sf.katta.protocol.operation.node.ShardUndeployOperation;
import net.sf.katta.util.CollectionUtil;

import org.apache.log4j.Logger;

public abstract class AbstractIndexOperation implements LeaderOperation {

  public static final char INDEX_SHARD_NAME_SEPARATOR = '#';
  private static final long serialVersionUID = 1L;
  protected final static Logger LOG = Logger.getLogger(AbstractIndexOperation.class);

  protected List<OperationId> distributeIndexShards(LeaderContext context, final IndexMetaData indexMD,
          Collection<String> liveNodes) throws IndexDeployException {
    if (liveNodes.isEmpty()) {
      throw new IndexDeployException(ErrorType.NO_NODES_AVAILIBLE, "no nodes availible");
    }

    InteractionProtocol protocol = context.getProtocol();
    Set<Shard> shards = indexMD.getShards();

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
        ShardUndeployOperation undeployInstruction = new ShardUndeployOperation(listOfRemoved);
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

  public static String createShardName(String indexName, String shardPath) {
    int lastIndexOf = shardPath.lastIndexOf("/");
    if (lastIndexOf == -1) {
      lastIndexOf = 0;
    }
    String shardFolderName = shardPath.substring(lastIndexOf + 1, shardPath.length());
    if (shardFolderName.endsWith(".zip")) {
      shardFolderName = shardFolderName.substring(0, shardFolderName.length() - 4);
    }
    return indexName + INDEX_SHARD_NAME_SEPARATOR + shardFolderName;
  }

  public static String getIndexNameFromShardName(String shardName) {
    return shardName.substring(0, shardName.indexOf(INDEX_SHARD_NAME_SEPARATOR));
  }

  protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol, IndexMetaData indexMD) {
    ReplicationReport replicationReport = protocol.getReplicationReport(indexMD);
    return canAndShouldRegulateReplication(protocol, replicationReport);
  }

  protected boolean canAndShouldRegulateReplication(InteractionProtocol protocol, ReplicationReport replicationReport) {
    List<String> liveNodes = protocol.getLiveNodes();
    if (replicationReport.isBalanced()) {
      return false;
    }
    if (replicationReport.isUnderreplicated()
            && liveNodes.size() <= replicationReport.getMinimalShardReplicationCount()) {
      return false;
    }
    return true;
  }

  protected void handleMasterDeployException(InteractionProtocol protocol, IndexMetaData indexMD, Exception e) {
    ErrorType errorType;
    if (e instanceof IndexDeployException) {
      errorType = ((IndexDeployException) e).getErrorType();
    } else {
      errorType = ErrorType.UNKNOWN;
    }
    IndexDeployError deployError = new IndexDeployError(indexMD.getName(), errorType);
    deployError.setException(e);
    indexMD.setDeployError(deployError);
    protocol.updateIndexMD(indexMD);
  }

  protected void handleDeploymentComplete(LeaderContext context, List<OperationResult> results, IndexMetaData indexMD,
          boolean newIndex) {
    ReplicationReport replicationReport = context.getProtocol().getReplicationReport(indexMD);
    if (replicationReport.isDeployed()) {
      indexMD.setDeployError(null);
      // we ignore possible shard errors
      if (canAndShouldRegulateReplication(context.getProtocol(), replicationReport)) {
        context.getProtocol().addLeaderOperation(new BalanceIndexOperation(indexMD.getName()));
      }
    } else {
      IndexDeployError deployError = new IndexDeployError(indexMD.getName(), ErrorType.SHARDS_NOT_DEPLOYABLE);
      for (OperationResult operationResult : results) {
        DeployResult deployResult = (DeployResult) operationResult;
        for (Entry<String, Exception> entry : deployResult.getShardExceptions()) {
          deployError.addShardError(entry.getKey(), entry.getValue());
        }
      }
      indexMD.setDeployError(deployError);
    }
    if (newIndex) {
      context.getProtocol().publishIndex(indexMD);
    } else {
      context.getProtocol().updateIndexMD(indexMD);
    }
  }

  static class IndexDeployException extends Exception {

    private static final long serialVersionUID = 1L;
    private final ErrorType _errorType;

    public IndexDeployException(ErrorType errorType, final String message) {
      super(message);
      _errorType = errorType;
    }

    public IndexDeployException(ErrorType errorType, final String message, final Throwable cause) {
      super(message, cause);
      _errorType = errorType;
    }

    public ErrorType getErrorType() {
      return _errorType;
    }
  }

}
