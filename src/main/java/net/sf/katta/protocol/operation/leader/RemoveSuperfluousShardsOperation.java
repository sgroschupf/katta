package net.sf.katta.protocol.operation.leader;

import net.sf.katta.master.LeaderContext;

public class RemoveSuperfluousShardsOperation implements LeaderOperation {

  private final String _nodeName;

  public RemoveSuperfluousShardsOperation(String nodeName) {
    _nodeName = nodeName;
  }

  @Override
  public void execute(LeaderContext context) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void nodeOperationsComplete(LeaderContext context) throws Exception {
    // nothing todo
  }

}
