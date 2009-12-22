package net.sf.katta.protocol.operation.node;

import java.io.Serializable;

import net.sf.katta.node.Node;
import net.sf.katta.node.NodeContext;

/**
 * An operation executed by an {@link Node}.
 */
public interface NodeOperation extends Serializable {

  /**
   * @param context
   * @return null or an {@link OperationResult}
   * @throws InterruptedException
   */
  OperationResult execute(NodeContext context) throws InterruptedException;
}
