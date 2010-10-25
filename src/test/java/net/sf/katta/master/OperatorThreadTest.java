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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.master.MasterOperation.ExecutionInstruction;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.testutil.Mocks;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.Matchers.notNull;

@SuppressWarnings("unchecked")
public class OperatorThreadTest {

  protected static final List EMPTY_LIST = Collections.EMPTY_LIST;

  private final InteractionProtocol _protocol = mock(InteractionProtocol.class);
  private final MasterQueue _queue = mock(MasterQueue.class);
  protected final MasterContext _context = new MasterContext(_protocol, Mocks.mockMaster(),
          new DefaultDistributionPolicy(), _queue);

  @Test(timeout = 10000)
  public void testSafeMode() throws Exception {
    final MasterOperation operation = mockOperation(ExecutionInstruction.EXECUTE);
    when(_queue.peek()).thenReturn(operation).thenAnswer(new SleepingAnswer());
    when(_protocol.getLiveNodes()).thenReturn(EMPTY_LIST);

    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();

    // no nodes connected
    Thread.sleep(safeModeMaxTime + 200);
    assertTrue(operatorThread.isAlive());
    assertTrue(operatorThread.isInSafeMode());

    // connect nodes
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    // check safe mode & operation execution
    Thread.sleep(safeModeMaxTime + 200);
    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    verify(operation, times(1)).execute(_context, EMPTY_LIST);
    operatorThread.interrupt();
  }

  @Test(timeout = 10000)
  public void testGracefulShutdownWhileInSafeMode() throws Exception {
    when(_queue.peek()).thenAnswer(new SleepingAnswer());

    long safeModeMaxTime = 2000;
    OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();

    assertTrue(operatorThread.isAlive());
    assertTrue(operatorThread.isInSafeMode());
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testGracefulShutdownWhileWaitingForOperations() throws Exception {
    when(_queue.peek()).thenAnswer(new SleepingAnswer());
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();

    waitUntilLeaveSafeMode(operatorThread);
    assertTrue(operatorThread.isAlive());
    assertFalse(operatorThread.isInSafeMode());
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOperationExecution() throws Exception {
    final MasterOperation masterOperation1 = mockOperation(ExecutionInstruction.EXECUTE);
    final MasterOperation masterOperation2 = mockOperation(ExecutionInstruction.EXECUTE);
    final MasterOperation masterOperation3 = mockOperation(ExecutionInstruction.EXECUTE);
    when(_queue.peek()).thenReturn(masterOperation1).thenReturn(masterOperation2).thenReturn(masterOperation3)
            .thenAnswer(new SleepingAnswer());

    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();
    waitUntilLeaveSafeMode(operatorThread);
    // Thread.sleep(safeModeMaxTime + 100);

    InOrder inOrder = inOrder(masterOperation1, masterOperation2, masterOperation3);
    inOrder.verify(masterOperation1, times(1)).execute(_context, EMPTY_LIST);
    inOrder.verify(masterOperation2, times(1)).execute(_context, EMPTY_LIST);
    inOrder.verify(masterOperation3, times(1)).execute(_context, EMPTY_LIST);
    operatorThread.interrupt();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testOperationWatchdog() throws Exception {
    String nodeName = "node1";
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList(nodeName));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(new OperationId(nodeName, "e1"));

    final MasterOperation leaderOperation = mock(MasterOperation.class);
    when(leaderOperation.execute(_context, EMPTY_LIST)).thenReturn(operationIds);
    setupExecutionInstruction(leaderOperation, ExecutionInstruction.EXECUTE);
    when(_queue.peek()).thenReturn(leaderOperation).thenAnswer(new SleepingAnswer());

    when(_protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(false);
    OperationWatchdog watchdog = mock(OperationWatchdog.class);
    when(_queue.moveOperationToWatching(leaderOperation, operationIds)).thenReturn(watchdog);

    // start the operator
    long safeModeMaxTime = 200;
    final OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();
    waitUntilLeaveSafeMode(operatorThread);

    verify(watchdog).start(_context);
    operatorThread.interrupt();
    operatorThread.join();
  }

  private void waitUntilLeaveSafeMode(final OperatorThread operatorThread) throws Exception {
    TestUtil.waitUntil(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return operatorThread.isInSafeMode();
      }
    }, TimeUnit.SECONDS, 30);
  }

  @Test(timeout = 10000)
  public void testOperationLocks_CancelLockedOperation() throws Exception {
    final MasterOperation leaderOperation1 = mock(MasterOperation.class);
    final MasterOperation leaderOperation2 = mock(MasterOperation.class);
    ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.CANCEL;

    runLockSituation(leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context, EMPTY_LIST);
  }

  @Test(timeout = 10000)
  public void testOperationLocks_SuspendLockedTask() throws Exception {
    final MasterOperation leaderOperation1 = mock(MasterOperation.class);
    final MasterOperation leaderOperation2 = mock(MasterOperation.class);

    ExecutionInstruction lockInstruction = MasterOperation.ExecutionInstruction.ADD_TO_QUEUE_TAIL;
    runLockSituation(leaderOperation1, leaderOperation2, lockInstruction);
    verify(leaderOperation2, times(0)).execute(_context, EMPTY_LIST);
    verify(_queue, times(1)).add(leaderOperation2);
  }

  @Test(timeout = 10000)
  public void testRecreateWatchdogs() throws Exception {
    when(_queue.peek()).thenAnswer(new SleepingAnswer());
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    OperationWatchdog watchdog1 = mock(OperationWatchdog.class);
    OperationWatchdog watchdog2 = mock(OperationWatchdog.class);
    when(watchdog1.isDone()).thenReturn(true);
    when(watchdog2.isDone()).thenReturn(false);
    when(_queue.getWatchdogs()).thenReturn(Arrays.asList(watchdog1, watchdog2));

    OperatorThread operatorThread = new OperatorThread(_context, 100);
    operatorThread.start();
    waitUntilLeaveSafeMode(operatorThread);
    Thread.sleep(200);

    verify(_queue).getWatchdogs();
    verify(watchdog1).isDone();
    verify(watchdog2).isDone();
    verify(_queue, times(1)).removeWatchdog(watchdog1);
    verify(_queue, times(0)).removeWatchdog(watchdog2);
    assertTrue(operatorThread.getOperationRegistry().getRunningOperations().contains(watchdog2.getOperation()));
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Queue() throws Exception {
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    when(_queue.peek()).thenThrow(new InterruptedException());
    OperatorThread operatorThread = new OperatorThread(_context, 50);
    operatorThread.start();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Queue_Zk() throws Exception {
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    ZkInterruptedException zkInterruptedException = mock(ZkInterruptedException.class);
    when(_queue.peek()).thenThrow(zkInterruptedException);
    OperatorThread operatorThread = new OperatorThread(_context, 50);
    operatorThread.start();
    operatorThread.join();
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Operation() throws Exception {
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    final MasterOperation masterOperation = mockOperation(ExecutionInstruction.EXECUTE);
    when(masterOperation.execute(_context, EMPTY_LIST)).thenThrow(new InterruptedException());
    when(_queue.peek()).thenReturn(masterOperation);
    OperatorThread operatorThread = new OperatorThread(_context, 50);
    operatorThread.start();
    operatorThread.join();
  }

  @Test(timeout = 1000000)
  public void testInterruptedException_Operation_Zk() throws Exception {
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    ZkInterruptedException zkInterruptedException = mock(ZkInterruptedException.class);
    final MasterOperation masterOperation = mockOperation(ExecutionInstruction.EXECUTE);
    when(masterOperation.execute(_context, EMPTY_LIST)).thenThrow(zkInterruptedException);
    when(_queue.peek()).thenReturn(masterOperation);
    OperatorThread operatorThread = new OperatorThread(_context, 50);
    operatorThread.start();
    operatorThread.join();
  }

  @Test(timeout = 10000000)
  public void testDontStopOnOOM() throws Exception {
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));
    when(_queue.peek()).thenThrow(new OutOfMemoryError("test exception")).thenAnswer(new SleepingAnswer());

    OperatorThread operatorThread = new OperatorThread(_context, 50);
    operatorThread.start();
    operatorThread.join(100);

    assertEquals(true, operatorThread.isAlive());
    operatorThread.interrupt();
    verify(_queue, atLeast(2)).peek();
  }

  private void runLockSituation(final MasterOperation leaderOperation1, final MasterOperation leaderOperation2,
          ExecutionInstruction instruction) throws Exception, InterruptedException {
    String nodeName = "node1";
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList(nodeName));
    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(new OperationId(nodeName, "e1"));

    when(_protocol.isNodeOperationQueued(operationIds.get(0))).thenReturn(false);
    OperationWatchdog watchdog = mock(OperationWatchdog.class);
    when(_queue.moveOperationToWatching(leaderOperation1, operationIds)).thenReturn(watchdog);

    setupExecutionInstruction(leaderOperation1, ExecutionInstruction.EXECUTE);
    setupExecutionInstruction(leaderOperation2, instruction);
    when(leaderOperation1.execute(_context, EMPTY_LIST)).thenReturn(operationIds);
    when(_queue.peek()).thenReturn(leaderOperation1).thenReturn(leaderOperation2).thenAnswer(new SleepingAnswer());

    // start the operator
    long safeModeMaxTime = 200;
    OperatorThread operatorThread = new OperatorThread(_context, safeModeMaxTime);
    operatorThread.start();

    // let operation1 be executed
    waitUntilLeaveSafeMode(operatorThread);
    // Thread.sleep(safeModeMaxTime + 100);
    verify(leaderOperation1, times(1)).execute(_context, EMPTY_LIST);

    operatorThread.interrupt();
    operatorThread.join();
  }

  private MasterOperation mockOperation(ExecutionInstruction instruction) throws Exception {
    MasterOperation masterOperation = mock(MasterOperation.class);
    when(masterOperation.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(instruction);
    return masterOperation;
  }

  private void setupExecutionInstruction(final MasterOperation leaderOperation, ExecutionInstruction instruction)
          throws Exception {
    when(leaderOperation.getExecutionInstruction((List<MasterOperation>) notNull())).thenReturn(instruction);
  }

}
