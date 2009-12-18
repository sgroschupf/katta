package net.sf.katta.protocol.operation.leader;

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
    List<String> liveNodes = protocol.getNodes();
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
