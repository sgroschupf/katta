package net.sf.katta.node;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import net.sf.katta.AbstractZkTest;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.testutil.mockito.SerializableCountDownLatchAnswer;

import org.junit.Test;

public class NodeZkTest extends AbstractZkTest {

  @Test
  public void testShutdown_shouldCleanupZkClientSubscriptions() {
    int numberOfListeners = _zk.getZkClient().numberOfListeners();
    Node node = new Node(_zk.getInteractionProtocol(), new LuceneServer());
    node.start();
    node.shutdown();
    assertEquals(numberOfListeners, _zk.getZkClient().numberOfListeners());
  }

  @Test(timeout = 10000)
  public void testNodeOperationPickup() throws Exception {
    Node node = new Node(_zk.getInteractionProtocol(), new LuceneServer());
    node.start();

    NodeOperation operation1 = mock(NodeOperation.class, withSettings().serializable());
    NodeOperation operation2 = mock(NodeOperation.class, withSettings().serializable());

    SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);
    when(operation1.execute((NodeContext) notNull())).thenAnswer(answer);
    when(operation2.execute((NodeContext) notNull())).thenAnswer(answer);
    _protocol.addNodeOperation(node.getName(), operation1);
    _protocol.addNodeOperation(node.getName(), operation2);
    answer.getCountDownLatch().await();

    node.shutdown();
  }
}
