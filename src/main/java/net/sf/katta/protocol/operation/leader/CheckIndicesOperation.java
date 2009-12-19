package net.sf.katta.protocol.operation.leader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.DeployResult;

public class CheckIndicesOperation extends AbstractIndexOperation {

  private static final long serialVersionUID = 1L;

  @Override
  public List<OperationId> execute(LeaderContext context) throws Exception {
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
  public LockInstruction getLockAlreadyObtainedInstruction() {
    return LockInstruction.ADD_TO_QUEUE_TAIL;
  }

  @Override
  public boolean locksOperation(LeaderOperation operation) {
    return operation instanceof CheckIndicesOperation;
  }

  @Override
  public void nodeOperationsComplete(LeaderContext context, List<DeployResult> results) throws Exception {
    // nothing to do
  }
}
