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

import java.util.List;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.OperationResult;

import org.I0Itec.zkclient.ExceptionUtil;

public class BalanceIndexOperation extends AbstractIndexOperation {

  private static final long serialVersionUID = 1L;
  private final String _indexName;

  public BalanceIndexOperation(String indexName) {
    _indexName = indexName;
  }

  @Override
  public List<OperationId> execute(LeaderContext context, List<LeaderOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    IndexMetaData indexMD = protocol.getIndexMD(_indexName);
    if (!canAndShouldRegulateReplication(protocol, _indexName)) {
      LOG.info("skip balancing for index '" + _indexName + "' cause there is no possible optimization");
      return null;
    }

    LOG.info("balancing shards for index '" + _indexName + "'");
    try {
      List<OperationId> operationIds = distributeIndexShards(context, indexMD, protocol.getLiveNodes());
      return operationIds;
    } catch (Exception e) {
      ExceptionUtil.rethrowInterruptedException(e);
      LOG.error("failed to deploy balance " + _indexName, e);
      handleMasterDeployException(protocol, indexMD, e);
      return null;
    }
  }

  @Override
  public void nodeOperationsComplete(LeaderContext context, List<OperationResult> results) throws Exception {
    LOG.info("balancing of index " + _indexName + " complete");
    handleDeploymentComplete(context, results, context.getProtocol().getIndexMD(_indexName), false);
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<LeaderOperation> runningOperations) throws Exception {
    for (LeaderOperation operation : runningOperations) {
      if (operation instanceof BalanceIndexOperation
              && ((BalanceIndexOperation) operation)._indexName.equals(_indexName)) {
        return ExecutionInstruction.CANCEL;
      }
    }
    return ExecutionInstruction.EXECUTE;
  }

}
