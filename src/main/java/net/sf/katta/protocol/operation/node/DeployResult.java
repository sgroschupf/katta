package net.sf.katta.protocol.operation.node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class DeployResult extends OperationResult {

  private static final long serialVersionUID = 1L;

  private Map<String, Exception> _exceptionByShard = new HashMap<String, Exception>(3);

  public DeployResult(String nodeName) {
    super(nodeName);
  }

  public void addShardException(String shardName, Exception exception) {
    _exceptionByShard.put(shardName, exception);
  }

  public Set<Entry<String, Exception>> getShardExceptions() {
    return _exceptionByShard.entrySet();
  }

}
