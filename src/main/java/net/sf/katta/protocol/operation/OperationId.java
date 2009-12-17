package net.sf.katta.protocol.operation;

public class OperationId {

  private final String _nodeName;
  private final String _elementName;

  public OperationId(String nodeName, String elementName) {
    _nodeName = nodeName;
    _elementName = elementName;
  }

  public String getNodeName() {
    return _nodeName;
  }

  public String getElementName() {
    return _elementName;
  }

  @Override
  public String toString() {
    return _nodeName + "-" + _elementName;
  }

}
