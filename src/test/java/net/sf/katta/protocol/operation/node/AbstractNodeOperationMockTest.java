package net.sf.katta.protocol.operation.node;

import static org.mockito.Mockito.mock;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeContext;
import net.sf.katta.node.ShardManager;
import net.sf.katta.protocol.InteractionProtocol;

public class AbstractNodeOperationMockTest {

  protected InteractionProtocol _protocol = mock(InteractionProtocol.class);
  protected Node _node = mock(Node.class);
  protected ShardManager _shardManager = mock(ShardManager.class);
  protected INodeManaged _nodeManaged = mock(INodeManaged.class);
  protected NodeContext _context = new NodeContext(_protocol, _node, _shardManager, _nodeManaged);

}
