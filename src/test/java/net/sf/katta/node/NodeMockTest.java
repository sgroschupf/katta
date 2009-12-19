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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.junit.Test;

public class NodeMockTest {

  private InteractionProtocol _protocol = mock(InteractionProtocol.class);
  private INodeManaged _nodeManaged = mock(INodeManaged.class);
  private Node _node = new Node(_protocol, _nodeManaged);

  @Test
  public void testGracefulStartup_Shutdown() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    OperationQueue<NodeOperation> queue = mock(OperationQueue.class);
    when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());
    when(_protocol.publishNode(eq(_node), (NodeMetaData) notNull())).thenReturn(queue);

    _node.start();
    assertNotNull(_node.getName());
    assertNotNull(_node.getRpcServer());
    assertTrue(_node.getRPCServerPort() > 0);
    verify(_nodeManaged).setNodeName((String) notNull());
    Thread.sleep(200);
    verify(nodeOperation).execute((NodeContext) notNull());

    _node.shutdown();
    verify(_protocol).unregisterComponent(_node);
    verify(_nodeManaged).shutdown();
  }

  @Test
  public void testDisconnectReconnect() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    OperationQueue<NodeOperation> queue = mock(OperationQueue.class);
    when(queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());
    when(_protocol.publishNode(eq(_node), (NodeMetaData) notNull())).thenReturn(queue).thenReturn(queue);

    _node.start();
    Thread.sleep(200);
    verify(nodeOperation, times(1)).execute((NodeContext) notNull());

    _node.disconnect();
    NodeOperation nodeOperation2 = mock(NodeOperation.class);
    reset(queue);
    when(queue.peek()).thenReturn(nodeOperation2).thenAnswer(new SleepingAnswer());

    _node.reconnect();
    Thread.sleep(200);
    verify(nodeOperation, times(1)).execute((NodeContext) notNull());
    verify(nodeOperation2, times(1)).execute((NodeContext) notNull());
  }

  public void testShutdown_doesNotCloseZkClient() {
    _node.start();
    _node.shutdown();
    verify(_protocol, never()).disconnect();
  }

}
