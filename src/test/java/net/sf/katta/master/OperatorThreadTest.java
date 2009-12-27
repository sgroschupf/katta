/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.Mocks;
import net.sf.katta.node.Node;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.AbstractMasterNodeZkTest;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.master.MasterOperation.ExecutionInstruction;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.junit.Test;
import org.mockito.InOrder;

public class OperatorThreadTest extends AbstractMasterNodeZkTest {

  @Test(timeout = 10000)
  public void testSafeMode() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final MasterOperation operation = mock(MasterOperation.class);
    when(queue.peek()).thenReturn(operation).thenAnswer(new SleepingAnswer());
    when(operation.getExecutionInstruction(EMPTY_LIST)).thenReturn(ExecutionInstruction.EXECUTE).thenAnswer(
            new SleepingAnswer());

    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    // no nodes connected
    Thread.sleep(safeModeMaxTime + 100);
    assertTrue(operatorThread.isAlive());
    assertTrue(operatorThread.isInSafeMode());

    // connect nodes
    Node node = Mocks.mockNode();
    Mocks.publisNode(_protocol, node);

    // check safe mode & operation execution
    Thread.sleep(safeModeMaxTime + 100);
    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    verify(operation, times(1)).execute(_context, EMPTY_LIST);
    operatorThread.interrupt();
  }

  @Test(timeout = 10000)
  public void testGracefulShutdownWhileInSleepMode() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.peek()).thenAnswer(new SleepingAnswer());

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
    when(queue.peek()).thenAnswer(new SleepingAnswer());

    Node node = Mocks.mockNode();
    Mocks.publisNode(_protocol, node);
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
    final MasterOperation masterOperation1 = mock(MasterOperation.class);
    final MasterOperation masterOperation2 = mock(MasterOperation.class);
    final MasterOperation masterOperation3 = mock(MasterOperation.class);
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.peek()).thenReturn(masterOperation1).thenReturn(masterOperation2).thenReturn(masterOperation3)
            .thenAnswer(new SleepingAnswer());
    when(masterOperation1.getExecutionInstruction(EMPTY_LIST)).thenReturn(ExecutionInstruction.EXECUTE).thenAnswer(
            new SleepingAnswer());
    when(masterOperation2.getExecutionInstruction(EMPTY_LIST)).thenReturn(ExecutionInstruction.EXECUTE).thenAnswer(
            new SleepingAnswer());
    when(masterOperation3.getExecutionInstruction(EMPTY_LIST)).thenReturn(ExecutionInstruction.EXECUTE).thenAnswer(
            new SleepingAnswer());

    Node node = Mocks.mockNode();
    Mocks.publisNode(_protocol, node);
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    Thread.sleep(safeModeMaxTime + 100);
    InOrder inOrder = inOrder(masterOperation1, masterOperation2, masterOperation3);
    inOrder.verify(masterOperation1, times(1)).execute(_context, EMPTY_LIST);
    inOrder.verify(masterOperation2, times(1)).execute(_context, EMPTY_LIST);
    inOrder.verify(masterOperation3, times(1)).execute(_context, EMPTY_LIST);
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOnNodeOperationCompletion() throws Exception {
    Node node = Mocks.mockNode();
    OperationQueue<NodeOperation> nodeQueue = Mocks.publisNode(_protocol, node);
    OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class, withSettings()
            .serializable()));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(operationId);

    final MasterOperation leaderOperation = mock(MasterOperation.class);
    when(leaderOperation.execute(_context, EMPTY_LIST)).thenReturn(operationIds);
    when(leaderOperation.getExecutionInstruction(EMPTY_LIST)).thenReturn(ExecutionInstruction.EXECUTE).thenAnswer(
            new SleepingAnswer());
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.peek()).thenReturn(leaderOperation).thenAnswer(new SleepingAnswer());

    // start the ooperator
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation, times(1)).execute(_context, EMPTY_LIST);
    verify(leaderOperation, times(0)).nodeOperationsComplete(eq(_context), (List) notNull());

    // complete node operation
    nodeQueue.remove();
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation, times(1)).nodeOperationsComplete(eq(_context), (List) notNull());

    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOperationLocks_CancelLockedOperation() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final MasterOperation leaderOperation1 = mock(MasterOperation.class);
    final MasterOperation leaderOperation2 = mock(MasterOperation.class);
    ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.CANCEL;
    runLockSituation(queue, leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context, EMPTY_LIST);
  }

  @Test(timeout = 10000)
  public void testOperationLocks_SuspendLockedTask() throws Exception {
    OperationQueue queue = mock(OperationQueue.class);
    final MasterOperation leaderOperation1 = mock(MasterOperation.class);
    final MasterOperation leaderOperation2 = mock(MasterOperation.class);

    ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.ADD_TO_QUEUE_TAIL;
    runLockSituation(queue, leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context, EMPTY_LIST);
    verify(queue, times(1)).add(leaderOperation2);
  }

  private void runLockSituation(OperationQueue queue, final MasterOperation leaderOperation1,
          final MasterOperation leaderOperation2, ExecutionInstruction instruction) throws Exception,
          InterruptedException {
    Node node = Mocks.mockNode();
    OperationQueue<NodeOperation> nodeQueue = Mocks.publisNode(_protocol, node);
    OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class, withSettings()
            .serializable()));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(operationId);

    when(leaderOperation1.execute(_context, EMPTY_LIST)).thenReturn(operationIds);
    when(leaderOperation1.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(
            ExecutionInstruction.EXECUTE);
    when(leaderOperation2.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(instruction);
    when(queue.peek()).thenReturn(leaderOperation1).thenReturn(leaderOperation2).thenAnswer(new SleepingAnswer());

    // start the ooperator
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, queue, safeModeMaxTime);
    operatorThread.start();

    // let operation1 be executed
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation1, times(1)).execute(_context, EMPTY_LIST);

    // complete node operation
    nodeQueue.remove();

    // let operation2 be executed
    Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation1, times(1)).nodeOperationsComplete(eq(_context), (List) notNull());
    operatorThread.interrupt();
    operatorThread.join();
  }

}
