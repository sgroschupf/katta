package net.sf.katta.operation.master;

import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;

import org.I0Itec.zkclient.ExceptionUtil;

@SuppressWarnings("serial")
public class IndexReinitializeOperation extends IndexDeployOperation {

  public IndexReinitializeOperation(IndexMetaData indexMD) {
    super(indexMD.getName(), indexMD.getPath(), indexMD.getReplicationLevel());
  }

  @Override
  public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    try {
      _indexMD.getShards().addAll(readShardsFromFs(_indexMD.getName(), _indexMD.getPath()));
      protocol.updateIndexMD(_indexMD);
    } catch (Exception e) {
      ExceptionUtil.rethrowInterruptedException(e);
      handleMasterDeployException(protocol, _indexMD, e);
    }
    return null;
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
    return ExecutionInstruction.EXECUTE;
  }

  @Override
  public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
    // nothing todo
  }

}
