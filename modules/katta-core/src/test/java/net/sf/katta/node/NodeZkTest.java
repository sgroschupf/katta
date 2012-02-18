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

import net.sf.katta.AbstractZkTest;
import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.testutil.mockito.SerializableCountDownLatchAnswer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import static org.mockito.Matchers.notNull;

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

  @Test(timeout = 20000)
  public void testNodeOperationPickup_AfterReconnect() throws Exception {
    Node node = new Node(_zk.getInteractionProtocol(), new LuceneServer());
    node.start();

    node.disconnect();
    node.reconnect();
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

  @Test(timeout = 2000000)
  public void testNodeReconnectWithInterruptSwallowingOperation() throws Exception {
    Node node = new Node(_zk.getInteractionProtocol(), new LuceneServer());
    node.start();
    _protocol.addNodeOperation(node.getName(), new InterruptSwallowingOperation());
    Thread.sleep(200);
    node.disconnect();
    node.reconnect();

    node.shutdown();
  }

  private static class InterruptSwallowingOperation implements NodeOperation {

    private static final long serialVersionUID = 1L;

    @Override
    public OperationResult execute(NodeContext context) throws InterruptedException {
      try {
        System.out.println("NodeZkTest.InterruptSwallowingOperation.execute()- entering sleep");
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        System.out.println("NodeZkTest.InterruptSwallowingOperation.execute()- leaving sleep");
      }
      return null;
    }

  }

  @Test(timeout = 10000)
  public void testNodeOperationException() throws Exception {
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
