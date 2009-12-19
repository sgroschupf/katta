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
package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.ZkTestSystem;

import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Rule;
import org.junit.Test;

public class InteractionProtocolTest {

  @Rule
  public ZkTestSystem _zk = ZkTestSystem.getInstance();
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  @Test(timeout = 7000)
  public void testLifecycle() throws Exception {
    int GATEWAY_PORT = 2190;
    Gateway gateway = new Gateway(GATEWAY_PORT, _zk.getServerPort());
    gateway.start();
    ZkClient zkClient = new ZkClient("localhost:" + GATEWAY_PORT);

    InteractionProtocol protocol = new InteractionProtocol(zkClient, _zk.getZkConf());
    final AtomicInteger connectCount = new AtomicInteger();
    final AtomicInteger disconnectCount = new AtomicInteger();
    final Object mutex = new Object();

    protocol.registerComponent(new ConnectedComponent() {
      @Override
      public void disconnect() {
        disconnectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }

      @Override
      public void reconnect() {
        connectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }
    });
    synchronized (mutex) {
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
    }
    zkClient.close();
    assertEquals(2, connectCount.get());
    assertEquals(2, connectCount.get());
  }

  @Test(timeout = 7000)
  public void testNodeQueue() throws Exception {
    Node node = mock(Node.class);
    String nodeName = "node1";
    when(node.getName()).thenReturn(nodeName);

    InteractionProtocol protocol = _zk.getInteractionProtocol();
    OperationQueue<NodeOperation> nodeQueue = protocol.publishNode(node, new NodeMetaData());

    NodeOperation nodeOperation1 = mock(NodeOperation.class, withSettings().serializable().name("a"));
    NodeOperation nodeOperation2 = mock(NodeOperation.class, withSettings().serializable().name("b"));
    OperationId operation1Id = protocol.addNodeOperation(nodeName, nodeOperation1);
    OperationId operation2Id = protocol.addNodeOperation(nodeName, nodeOperation2);

    assertTrue(protocol.isNodeOperationQueued(operation1Id));
    assertTrue(protocol.isNodeOperationQueued(operation2Id));
    assertEquals(nodeOperation1.toString(), nodeQueue.remove().toString());
    assertEquals(nodeOperation2.toString(), nodeQueue.remove().toString());
    assertTrue(nodeQueue.isEmpty());
    assertFalse(protocol.isNodeOperationQueued(operation1Id));
    assertFalse(protocol.isNodeOperationQueued(operation2Id));
  }
}
