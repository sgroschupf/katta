package net.sf.katta.protocol.operation.node;

import java.io.Serializable;

import net.sf.katta.node.Node;
import net.sf.katta.node.NodeContext;

/**
 * An operation executed by an {@link Node}.
 * 
 * @param <T>
 *          the type of the {@link OperationResult}
 */
public interface NodeOperation<T extends OperationResult> extends Serializable {

  /**
   * @param context
   * @return null or an {@link OperationResult}
   */
  T execute(NodeContext context);
}
