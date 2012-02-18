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

import java.io.File;
import java.io.IOException;

import net.sf.katta.AbstractTest;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.mockito.SleepingAnswer;
import net.sf.katta.util.NodeConfiguration;

import org.apache.hadoop.fs.FileUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;

public class NodeMockTest extends AbstractTest {

  private InteractionProtocol _protocol = mock(InteractionProtocol.class);
  private IContentServer _contentServer = mock(IContentServer.class);
  private Node _node = new Node(_protocol, _contentServer);
  private NodeQueue _queue = mock(NodeQueue.class);

  @Before
  public void setUp() throws IOException {
    File shardFolder = new NodeConfiguration().getShardFolder();
    FileUtil.fullyDelete(shardFolder);
    when(_protocol.publishNode(eq(_node), (NodeMetaData) notNull())).thenReturn(_queue);
  }

  @Test
  public void testGracefulStartup_Shutdown() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    when(_queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

    _node.start();

    assertNotNull(_node.getName());
    assertNotNull(_node.getRpcServer());
    assertTrue(_node.getRPCServerPort() > 0);
    verify(_contentServer).init((String) notNull(), (NodeConfiguration) notNull());
    Thread.sleep(200);
    verify(nodeOperation).execute((NodeContext) notNull());

    _node.shutdown();
    verify(_protocol).unregisterComponent(_node);
    verify(_contentServer).shutdown();
  }

  @Test
  public void testDisconnectReconnect() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    when(_queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());
    _node.start();
    Thread.sleep(200);
    verify(nodeOperation, times(1)).execute((NodeContext) notNull());

    _node.disconnect();
    NodeOperation nodeOperation2 = mock(NodeOperation.class);
    reset(_queue);
    when(_queue.peek()).thenReturn(nodeOperation2).thenAnswer(new SleepingAnswer());

    _node.reconnect();
    Thread.sleep(200);
    verify(nodeOperation, times(1)).execute((NodeContext) notNull());
    verify(nodeOperation2, times(1)).execute((NodeContext) notNull());
    _node.shutdown();
  }

  @Test
  public void testShutdown_doesNotCloseZkClient() throws Exception {
    when(_queue.peek()).thenAnswer(new SleepingAnswer());
    _node.start();
    _node.shutdown();
    verify(_protocol, never()).disconnect();
  }

  @Test
  public void testRedployInstalledShards() throws Exception {
    NodeOperation nodeOperation = mock(NodeOperation.class);
    when(_queue.peek()).thenReturn(nodeOperation).thenAnswer(new SleepingAnswer());

    // start and add shard
    _node.start();
    verify(_contentServer, times(0)).addShard(anyString(), any(File.class));
    String shardName = "shard1";
    File shardFile = TestResources.SHARD1;
    _node.getContext().getShardManager().installShard(shardName, shardFile.getAbsolutePath());

    // restart, node should be added
    _node.shutdown();
    _node = new Node(_protocol, _contentServer);
    when(_protocol.publishNode(eq(_node), (NodeMetaData) notNull())).thenReturn(_queue);
    _node.start();
    verify(_contentServer, times(1)).addShard(anyString(), any(File.class));
    verify(_protocol).publishShard(eq(_node), eq(shardName));
    _node.shutdown();
  }

}
