package net.sf.katta.protocol.operation.leader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.OperationResult;
import net.sf.katta.protocol.operation.node.ShardUndeployOperation;

public class RemoveSuperfluousShardsOperation implements LeaderOperation {

  private static final long serialVersionUID = 1L;
  private final String _nodeName;

  public RemoveSuperfluousShardsOperation(String nodeName) {
    _nodeName = nodeName;
  }

  public String getNodeName() {
    return _nodeName;
  }

  @Override
  public List<OperationId> execute(LeaderContext context, List<LeaderOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    Collection<String> nodeShards = protocol.getNodeShards(_nodeName);
    List<String> obsoletShards = collectObsoleteShards(protocol, nodeShards, runningOperations);
    if (!obsoletShards.isEmpty()) {
      protocol.addNodeOperation(_nodeName, new ShardUndeployOperation(obsoletShards));
    }

    return null;
  }

  private List<String> collectObsoleteShards(InteractionProtocol protocol, Collection<String> nodeShards,
          List<LeaderOperation> runningOperations) {
    List<String> obsoletShards = new ArrayList<String>();
    for (String shardName : nodeShards) {
      String indexName = AbstractIndexOperation.getIndexNameFromShardName(shardName);
      IndexMetaData indexMD = protocol.getIndexMD(indexName);
      if (indexMD == null && !containsDeployOperation(runningOperations, indexName)) {
        // index has been removed
        obsoletShards.add(shardName);
      }
    }
    return obsoletShards;
  }

  private boolean containsDeployOperation(List<LeaderOperation> runningOperations, String indexName) {
    for (LeaderOperation leaderOperation : runningOperations) {
      if (leaderOperation instanceof IndexDeployOperation
              && ((IndexDeployOperation) leaderOperation).getIndexName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void nodeOperationsComplete(LeaderContext context, List<OperationResult> results) throws Exception {
    // nothing todo
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<LeaderOperation> runningOperations) throws Exception {
    return ExecutionInstruction.EXECUTE;
  }

}
