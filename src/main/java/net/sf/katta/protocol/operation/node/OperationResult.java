package net.sf.katta.protocol.operation.node;

import java.io.Serializable;

public class OperationResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String _nodeName;

  public OperationResult(String nodeName) {
    _nodeName = nodeName;
  }

  public String getNodeName() {
    return _nodeName;
  }

}
