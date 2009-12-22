package net.sf.katta.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.leader.MockedMasterNodeTest;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.OperationResult;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class OperationRegistryTest extends MockedMasterNodeTest {

  private final OperationRegistry _registry = new OperationRegistry(_context);
  private final List<Node> _nodes = mockNodes(5);
  private final LeaderOperation _leaderOperation = mock(LeaderOperation.class);

  @Test(timeout = 10000)
  public void testAllOperationsDoneWithoutResults() throws Exception {
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog operationWatchdog = beginLeaderOperation(_nodes, _leaderOperation);

    // execute node operations
    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    for (OperationQueue<NodeOperation> nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.peek();
      assertNotNull(nodeOperation);
      nodeQueue.remove();
      operationResults.add(null);
    }
    operationWatchdog.join();
    assertTrue(operationWatchdog.isDone());
    assertEquals(0, operationWatchdog.getOpenOperationCount());
    verify(_leaderOperation, times(1)).nodeOperationsComplete(_context, operationResults);
  }

  @Test(timeout = 10000)
  public void testAllOperationsDone() throws Exception {
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog operationWatchdog = beginLeaderOperation(_nodes, _leaderOperation);

    // execute node operations
    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    int i = 0;
    for (OperationQueue<NodeOperation> nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.peek();
      assertNotNull(nodeOperation);
      OperationResult result = new OperationResult(_nodes.get(i).getName());
      operationResults.add(result);
      nodeQueue.remove(result);
      i++;
    }
    operationWatchdog.join();
    assertTrue(operationWatchdog.isDone());
    assertEquals(0, operationWatchdog.getOpenOperationCount());
    ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);

    verify(_leaderOperation, times(1)).nodeOperationsComplete(eq(_context), argument.capture());
    List<OperationResult> capturedResults = argument.getValue();
    assertEquals(operationResults.size(), capturedResults.size());
    for (OperationResult result : capturedResults) {
      assertNotNull(result);
      assertNotNull(result.getNodeName());
    }
  }

  @Test(timeout = 10000)
  public void testOneOperationsMissing() throws Exception {
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog watchdog = beginLeaderOperation(_nodes, _leaderOperation);

    // execute operation
    nodeQueues.get(0).remove();
    nodeQueues.get(1).remove();
    nodeQueues.get(2).remove();

    // deployment be still pending
    watchdog.join(250);
    assertFalse(watchdog.isDone());
    assertEquals(2, watchdog.getOpenOperationCount());
    watchdog.cancel();
  }

  @Test(timeout = 10000)
  public void testOperationsDoneOrNodeGone() throws Exception {
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(_nodes);
    OperationWatchdog watchdog = beginLeaderOperation(_nodes, _leaderOperation);

    nodeQueues.get(0).remove();
    nodeQueues.get(1).remove();
    nodeQueues.get(2).remove();
    _protocol.unregisterComponent(_nodes.get(3));
    _protocol.unregisterComponent(_nodes.get(4));

    // deployment should be complete
    watchdog.join();
    assertTrue(watchdog.isDone());
    assertEquals(0, watchdog.getOpenOperationCount());
  }

  @Test(timeout = 10000)
  public void testGetRunningOperations() throws Exception {
    publisNodes(_nodes);
    beginLeaderOperation(_nodes, _leaderOperation);
    assertEquals(1, _registry.getRunningOperations().size());
    _registry.shutdown();
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
