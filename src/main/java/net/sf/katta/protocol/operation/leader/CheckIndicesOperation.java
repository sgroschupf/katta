package net.sf.katta.protocol.operation.leader;

import java.util.List;
import java.util.Set;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.master.OperationRegistry;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.operation.OperationId;

//TODO should be a global check index or everything op
public class CheckIndicesOperation extends AbstractIndexOperation {

  private static final long serialVersionUID = 1L;
  private final CheckType _checkType;

  public enum CheckType {
    UNDEREPLICATED, OVERREPLICATED, ALL;
  }

  public CheckIndicesOperation(CheckType checkType) {
    _checkType = checkType;
  }

  // TODO do we really need sub-operations
  @Override
  public List<OperationId> execute(LeaderContext context) throws Exception {
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
  public void nodeOperationsComplete(LeaderContext context) throws Exception {
    // nothing to do
  }
}
