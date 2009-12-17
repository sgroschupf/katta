package net.sf.katta.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.DistributedBlockingQueue;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.AbstractLeaderTest;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.node.NodeOperation;

import org.junit.Test;

public class OperationRegistryTest extends AbstractLeaderTest {

  private final OperationRegistry _registry = new OperationRegistry(_context);
  private final List<Node> _nodes = mockNodes(5);
  private final LeaderOperation _leaderOperation = mock(LeaderOperation.class);

  @Test(timeout = 10000)
  public void testAllOperationsDone() throws Exception {
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog operationWatchdog = beginLeaderOperation(_nodes, _leaderOperation);

    // execute node operations
    for (DistributedBlockingQueue<NodeOperation> nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.poll();
      assertNotNull(nodeOperation);
    }
    operationWatchdog.join();
    assertTrue(operationWatchdog.isDone());
    assertEquals(0, operationWatchdog.getOpenOperationCount());
    verify(_leaderOperation, times(1)).nodeOperationsComplete(_context);
  }

  @Test(timeout = 10000)
  public void testOneOperationsMissing() throws Exception {
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog watchdog = beginLeaderOperation(_nodes, _leaderOperation);

    // execute operation
    nodeQueues.get(0).poll();
    nodeQueues.get(1).poll();
    nodeQueues.get(2).poll();

    // deployment be still pending
    watchdog.join(250);
    assertFalse(watchdog.isDone());
    assertEquals(2, watchdog.getOpenOperationCount());
  }

  @Test(timeout = 10000)
  public void testOperationsDoneOrNodeGone() throws Exception {
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog watchdog = beginLeaderOperation(_nodes, _leaderOperation);

    nodeQueues.get(0).poll();
    nodeQueues.get(1).poll();
    nodeQueues.get(2).poll();
    _protocol.unregisterComponent(_nodes.get(3));
    _protocol.unregisterComponent(_nodes.get(4));

    // deployment should be complete
    watchdog.join();
    assertTrue(watchdog.isDone());
    assertEquals(0, watchdog.getOpenOperationCount());
  }

  @Test(timeout = 10000)
  public void testOperationsLock() throws Exception {
    LeaderOperation leaderOperation2 = mock(LeaderOperation.class);
    assertFalse(_registry.isLocked(leaderOperation2));

    publisNodes(_nodes);
    beginLeaderOperation(_nodes, _leaderOperation);
    assertFalse(_registry.isLocked(leaderOperation2));
    when(_leaderOperation.locksOperation(leaderOperation2)).thenReturn(true);
    assertTrue(_registry.isLocked(leaderOperation2));
  }

  private OperationWatchdog beginLeaderOperation(List<Node> nodes, LeaderOperation leaderOperation) {
    List<OperationId> operationIds = new ArrayList<OperationId>();
    for (Node node : nodes) {
      OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class));
      operationIds.add(operationId);
    }
    OperationWatchdog watchdog = _registry.watchFor(operationIds, leaderOperation);
    assertFalse(watchdog.isDone());
    assertEquals(nodes.size(), watchdog.getOpenOperationCount());
    return watchdog;
  }
}
