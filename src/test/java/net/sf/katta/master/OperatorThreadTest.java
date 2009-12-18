package net.sf.katta.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.MockedMasterNodeTest;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.leader.LeaderOperation.LockInstruction;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.testutil.SleepingAnswer;

import org.junit.Test;
import org.mockito.InOrder;

public class OperatorThreadTest extends MockedMasterNodeTest {

  @Test(timeout = 10000)
  public void testSafeMode() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final LeaderOperation leaderOperation = mock(LeaderOperation.class);
    when(queue.poll()).thenReturn(leaderOperation).thenAnswer(new SleepingAnswer());

    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    // no nodes connected
    Thread.sleep(safeModeMaxTime + 100);
    assertTrue(operatorThread.isAlive());
    assertTrue(operatorThread.isInSafeMode());

    // connect nodes
    Node node = mockNode();
    publisNode(node);

    // check safe mode & operation execution
    Thread.sleep(safeModeMaxTime + 100);
    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    verify(leaderOperation, times(1)).execute(_context);
    operatorThread.interrupt();
  }

  @Test(timeout = 10000)
  public void testGracefulShutdownWhileInSleepMode() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.poll()).thenAnswer(new SleepingAnswer());

    long safeModeMaxTime = 2000;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testGracefulShutdownWhileWaitingForOperations() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.poll()).thenAnswer(new SleepingAnswer());

    Node node = mockNode();
    publisNode(node);
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    Thread.sleep(safeModeMaxTime + 100);
    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOperationExecution() throws Exception {
    final LeaderOperation leaderOperation1 = mock(LeaderOperation.class);
    final LeaderOperation leaderOperation2 = mock(LeaderOperation.class);
    final LeaderOperation leaderOperation3 = mock(LeaderOperation.class);
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.poll()).thenReturn(leaderOperation1).thenReturn(leaderOperation2).thenReturn(leaderOperation3)
            .thenAnswer(new SleepingAnswer());

    Node node = mockNode();
    publisNode(node);
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    Thread.sleep(safeModeMaxTime + 100);
    InOrder inOrder = inOrder(leaderOperation1, leaderOperation2, leaderOperation3);
    inOrder.verify(leaderOperation1, times(1)).execute(_context);
    inOrder.verify(leaderOperation2, times(1)).execute(_context);
    inOrder.verify(leaderOperation3, times(1)).execute(_context);
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOnNodeOperationCompletion() throws Exception {
    Node node = mockNode();
    OperationQueue<NodeOperation> nodeQueue = publisNode(node);
    OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class, withSettings()
            .serializable()));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(operationId);

    final LeaderOperation leaderOperation = mock(LeaderOperation.class);
    when(leaderOperation.execute(_context)).thenReturn(operationIds);
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.poll()).thenReturn(leaderOperation).thenAnswer(new SleepingAnswer());

    // start the ooperator
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation, times(1)).execute(_context);
    verify(leaderOperation, times(0)).nodeOperationsComplete(_context);

    // complete node operation
    nodeQueue.poll();
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation, times(1)).nodeOperationsComplete(_context);

    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOperationLocks_CancelLockedOperation() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final LeaderOperation leaderOperation1 = mock(LeaderOperation.class);
    final LeaderOperation leaderOperation2 = mock(LeaderOperation.class);
    LockInstruction lockInstruction = LeaderOperation.LockInstruction.CANCEL_THIS_OPERATION;
    runLockSituation(queue, leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context);
  }

  @Test(timeout = 10000)
  public void testOperationLocks_SuspendLockedTask() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final LeaderOperation leaderOperation1 = mock(LeaderOperation.class);
    final LeaderOperation leaderOperation2 = mock(LeaderOperation.class);

    LockInstruction lockInstruction = LeaderOperation.LockInstruction.ADD_TO_QUEUE_TAIL;
    runLockSituation(queue, leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context);
    verify(queue, times(1)).add(leaderOperation2);
  }

  private void runLockSituation(OperationQueue queue, final LeaderOperation leaderOperation1,
          final LeaderOperation leaderOperation2, LockInstruction lockInstruction) throws Exception,
          InterruptedException {
    Node node = mockNode();
    OperationQueue<NodeOperation> nodeQueue = publisNode(node);
    OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class, withSettings()
            .serializable()));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(operationId);

    when(leaderOperation1.execute(_context)).thenReturn(operationIds);
    when(leaderOperation1.locksOperation(leaderOperation2)).thenReturn(true);
    when(leaderOperation2.getLockAlreadyObtainedInstruction()).thenReturn(lockInstruction);
    when(queue.poll()).thenReturn(leaderOperation1).thenReturn(leaderOperation2).thenAnswer(new SleepingAnswer());

    // start the ooperator
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    // let operation1 be executed
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation1, times(1)).execute(_context);

    // complete node operation
    nodeQueue.poll();

    // let operation2 be executed
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation1, times(1)).nodeOperationsComplete(_context);
    operatorThread.interrupt();
    operatorThread.join();
  }

}
