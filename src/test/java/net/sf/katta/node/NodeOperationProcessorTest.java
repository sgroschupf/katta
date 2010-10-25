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
package net.sf.katta.node;

import net.sf.katta.node.Node.NodeOperationProcessor;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeOperationProcessorTest {

  private NodeQueue _queue = mock(NodeQueue.class);
  private NodeContext _context = mock(NodeContext.class);
  private Node _node = mock(Node.class);
  private NodeOperationProcessor _processor;

  public NodeOperationProcessorTest() throws InterruptedException {
    when(_context.getNode()).thenReturn(_node);
    when(_node.isRunning()).thenReturn(true);
    when(_node.getName()).thenReturn("aNode");
    _processor = new NodeOperationProcessor(_queue, _context);
    // when(queue.peek()).thenAnswer(new SleepingAnswer());
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Queue() throws Exception {
    when(_queue.peek()).thenThrow(new InterruptedException());
    _processor.run();
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Queue_Zk() throws Exception {
    when(_queue.peek()).thenThrow(new ZkInterruptedException(new InterruptedException()));
    _processor.run();
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Operation() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    when(_queue.peek()).thenReturn(nodeOperation);
    when(nodeOperation.execute(_context)).thenThrow(new InterruptedException());
    _processor.run();
  }

  @Test(timeout = 10000)
  public void testInterruptedException_Operation_ZK() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    when(_queue.peek()).thenReturn(nodeOperation);
    when(nodeOperation.execute(_context)).thenThrow(new ZkInterruptedException(new InterruptedException()));
    _processor.run();
  }

  @Test(timeout = 10000)
  public void testDontStopOnOOM() throws Exception {
    when(_queue.peek()).thenThrow(new OutOfMemoryError("test exception")).thenAnswer(new SleepingAnswer());
    Thread thread = new Thread() {
      public void run() {
        _processor.run();
      };
    };
    thread.start();
    Thread.sleep(500);
    assertEquals(true, thread.isAlive());
    thread.interrupt();
    verify(_queue, atLeast(2)).peek();
  }
}
