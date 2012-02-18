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
import java.util.Collection;
import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;

import org.mortbay.log.Log;

public class RemoveObsoleteShardsOperation implements MasterOperation {

  private static final long serialVersionUID = 1L;
  private final String _nodeName;

  public RemoveObsoleteShardsOperation(String nodeName) {
    _nodeName = nodeName;
  }

  public String getNodeName() {
    return _nodeName;
  }

  @Override
  public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    Collection<String> nodeShards = protocol.getNodeShards(_nodeName);
    List<String> obsoletShards = collectObsoleteShards(protocol, nodeShards, runningOperations);
    if (!obsoletShards.isEmpty()) {
      Log.info("found following shards obsolete on node " + _nodeName + ": " + obsoletShards);
      protocol.addNodeOperation(_nodeName, new ShardUndeployOperation(obsoletShards));
    }

    return null;
  }

  private List<String> collectObsoleteShards(InteractionProtocol protocol, Collection<String> nodeShards,
          List<MasterOperation> runningOperations) {
    List<String> obsoletShards = new ArrayList<String>();
    for (String shardName : nodeShards) {
      try {
        String indexName = AbstractIndexOperation.getIndexNameFromShardName(shardName);
        IndexMetaData indexMD = protocol.getIndexMD(indexName);
        if (indexMD == null && !containsDeployOperation(runningOperations, indexName)) {
          // index has been removed
          obsoletShards.add(shardName);
        }
      } catch (IllegalArgumentException e) {
        Log.warn("found shard with invalid name '" + shardName + "' - instruct removal");
        obsoletShards.add(shardName);
      }
    }
    return obsoletShards;
  }

  private boolean containsDeployOperation(List<MasterOperation> runningOperations, String indexName) {
    for (MasterOperation masterOperation : runningOperations) {
      if (masterOperation instanceof IndexDeployOperation
              && ((IndexDeployOperation) masterOperation).getIndexName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
    // nothing todo
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
    return ExecutionInstruction.EXECUTE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode()) + ":" + _nodeName;
  }

}
