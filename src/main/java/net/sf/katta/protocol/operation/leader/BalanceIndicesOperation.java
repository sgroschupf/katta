package net.sf.katta.protocol.operation.leader;

import java.util.List;
import java.util.Set;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.master.OperationRegistry;
import net.sf.katta.protocol.InteractionProtocol;

//TODO should be a global check index or everything op
public class BalanceIndicesOperation extends AbstractInbalanceOperation {

  private static final long serialVersionUID = 1L;
  private final CheckType _checkType;

  public enum CheckType {
    UNDEREPLICATED, OVERREPLICATED, ALL;
  }

  public BalanceIndicesOperation(CheckType checkType) {
    _checkType = checkType;
  }

  // TODO do we really need sub-operations
  @Override
  public void execute(LeaderContext context) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    OperationRegistry registry = context.getOperationRegistry();
    List<String> liveNodes = protocol.getNodes();
    if (_checkType == CheckType.ALL || _checkType == CheckType.UNDEREPLICATED) {
      Set<String> underreplicatedIndexes = getUnderreplicatedIndexes(protocol, liveNodes.size());
      for (String index : underreplicatedIndexes) {
        if (!registry.isIndexBalancing(index)) {
          distributeIndexShards(protocol, context.getDeployPolicy(), protocol.getIndexMD(index), liveNodes, false);
        }
      }
    }
    if (_checkType == CheckType.ALL || _checkType == CheckType.OVERREPLICATED) {
      Set<String> overReplicatedIndices = getOverreplicatedIndexes(protocol);
      for (String index : overReplicatedIndices) {
        if (!registry.isIndexBalancing(index)) {
          distributeIndexShards(protocol, context.getDeployPolicy(), protocol.getIndexMD(index), liveNodes, false);
        }
      }
    }
  }

  @Override
  public void nodeOperationComplete(LeaderContext context) throws Exception {
    // nothing to do
  }
}
