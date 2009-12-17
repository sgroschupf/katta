package net.sf.katta.protocol.operation.node;

import java.io.Serializable;

import net.sf.katta.node.Node;
import net.sf.katta.node.NodeContext;

/**
 * Marker interface for operations executed by an {@link Node}.
 */
public interface NodeOperation extends Serializable {

  void execute(NodeContext context);
}
