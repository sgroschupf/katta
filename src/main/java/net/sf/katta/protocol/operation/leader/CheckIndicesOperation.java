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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.ReplicationReport;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.OperationResult;

public class CheckIndicesOperation extends AbstractIndexOperation {

  private static final long serialVersionUID = 1L;

  @Override
  public List<OperationId> execute(LeaderContext context, List<LeaderOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    List<String> liveNodes = protocol.getLiveNodes();
    addBalanceOperations(protocol, getUnderreplicatedIndexes(protocol, liveNodes.size()));
    addBalanceOperations(protocol, getOverreplicatedIndexes(protocol));
    // TODO jz: check for recoverble index errors (like no nodes)
    return null;
  }

  private void addBalanceOperations(InteractionProtocol protocol, Set<String> indices) {
    for (String index : indices) {
      if (canAndShouldRegulateReplication(protocol, index)) {
        BalanceIndexOperation balanceOperation = new BalanceIndexOperation(index);
        protocol.addLeaderOperation(balanceOperation);
      }
    }
  }

  protected Set<String> getUnderreplicatedIndexes(final InteractionProtocol protocol, int nodeCount) {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : protocol.getIndices()) {
      ReplicationReport replicationReport = getReplicationReport(protocol, index);
      if (replicationReport.isUnderreplicated() && nodeCount >= replicationReport.getMinimalShardReplicationCount()) {
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

  @Override
  public void nodeOperationsComplete(LeaderContext context, List<OperationResult> results) throws Exception {
    // nothing to do
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<LeaderOperation> runningOperations) throws Exception {
    for (LeaderOperation operation : runningOperations) {
      if (operation instanceof CheckIndicesOperation) {
        return ExecutionInstruction.ADD_TO_QUEUE_TAIL;
      }
    }
    return ExecutionInstruction.EXECUTE;
  }
}
