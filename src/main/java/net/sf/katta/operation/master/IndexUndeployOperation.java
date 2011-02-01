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
package net.sf.katta.operation.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.ZkClient;

public class IndexUndeployOperation implements MasterOperation {

  private static final long serialVersionUID = 1L;
  private final String _indexName;
  private IndexMetaData _indexMD;

  public IndexUndeployOperation(String indexName) {
    _indexName = indexName;
  }

  @Override
  public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    _indexMD = protocol.getIndexMD(_indexName);

    Map<String, List<String>> shard2NodesMap = protocol.getShard2NodesMap(Shard.getShardNames(_indexMD.getShards()));
    Map<String, List<String>> node2ShardsMap = CollectionUtil.invertListMap(shard2NodesMap);
    Set<String> nodes = node2ShardsMap.keySet();
    List<OperationId> nodeOperationIds = new ArrayList<OperationId>(nodes.size());
    for (String node : nodes) {
      List<String> nodeShards = node2ShardsMap.get(node);
      OperationId operationId = protocol.addNodeOperation(node, new ShardUndeployOperation(nodeShards));
      nodeOperationIds.add(operationId);
    }
    protocol.unpublishIndex(_indexName);
    return nodeOperationIds;
  }

  @Override
  public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
    ZkClient zkClient = context.getProtocol().getZkClient();
    ZkConfiguration zkConf = context.getProtocol().getZkConfiguration();
    for (Shard shard : _indexMD.getShards()) {
      zkClient.deleteRecursive(zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard.getName()));
    }
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
    for (MasterOperation operation : runningOperations) {
      if (operation instanceof IndexUndeployOperation
              && ((IndexUndeployOperation) operation)._indexName.equals(_indexName)) {
        return ExecutionInstruction.CANCEL;
      }
    }
    return ExecutionInstruction.EXECUTE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + _indexName;
  }

}
