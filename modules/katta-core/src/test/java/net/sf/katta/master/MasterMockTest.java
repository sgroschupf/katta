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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import net.sf.katta.AbstractTest;
import net.sf.katta.operation.master.CheckIndicesOperation;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.master.RemoveObsoleteShardsOperation;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.testutil.mockito.SleepingAnswer;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MasterMockTest extends AbstractTest {

  private InteractionProtocol _protocol = mock(InteractionProtocol.class);

  @Before
  public void setUp() {
    when(_protocol.getZkConfiguration()).thenReturn(new ZkConfiguration());
    ZkClient mock = mock(ZkClient.class);
    when(_protocol.getZkClient()).thenReturn(mock);
  }

  @Test
  public void testBecomeMaster() throws Exception {
    final Master master = new Master(_protocol, false);

    MasterQueue masterQueue = mockBlockingOperationQueue();
    when(_protocol.publishMaster(master)).thenReturn(masterQueue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    assertTrue(master.isMaster());
    master.shutdown();
    assertFalse(master.isMaster());
  }

  @Test
  public void testBecomeSecMaster() throws Exception {
    final Master master = new Master(_protocol, false);

    when(_protocol.publishMaster(master)).thenReturn(null);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    assertFalse(master.isMaster());
    master.shutdown();
  }

  @Test
  public void testDisconnectReconnect() throws Exception {
    final Master master = new Master(_protocol, false);

    MasterQueue masterQueue = mockBlockingOperationQueue();
    when(_protocol.publishMaster(master)).thenReturn(masterQueue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    assertTrue(master.isMaster());

    master.disconnect();
    assertFalse(master.isMaster());

    master.reconnect();
    assertTrue(master.isMaster());

    master.shutdown();
  }

  @Test
  public void testGracefulStartupShutdown() throws Exception {
    boolean shutdownClient = false;
    checkStartStop(shutdownClient, null);
  }

  @Test
  public void testGracefulStartupShutdownWithShutdownClient() throws Exception {
    boolean shutdownClient = true;
    checkStartStop(shutdownClient, null);
  }

  @Test
  public void testGracefulStartupShutdownWithZkServer() throws Exception {
    ZkServer zkServer = mock(ZkServer.class);
    checkStartStop(false, zkServer);
  }

  @Test
  public void testRemoveOldNodeShards() throws Exception {
    final Master master = new Master(_protocol, false);

    String nodeName = "node1";
    MasterQueue masterQueue = mockBlockingOperationQueue();
    when(_protocol.publishMaster(master)).thenReturn(masterQueue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList(nodeName));
    when(_protocol.registerChildListener(eq(master), eq(PathDef.NODES_LIVE), any(IAddRemoveListener.class)))
            .thenReturn(Arrays.asList(nodeName));

    List<String> shards = Arrays.asList("shard1", "shard2");
    when(_protocol.getNodeShards(nodeName)).thenReturn(shards);
    master.start();

    assertTrue(master.isMaster());
    TestUtil.waitUntilLeaveSafeMode(master);
    ArgumentCaptor<MasterOperation> argument = ArgumentCaptor.forClass(MasterOperation.class);
    verify(_protocol, times(2)).addMasterOperation(argument.capture());
    assertTrue(argument.getAllValues().get(0) instanceof CheckIndicesOperation);
    assertTrue(argument.getAllValues().get(1) instanceof RemoveObsoleteShardsOperation);
    assertEquals(nodeName, ((RemoveObsoleteShardsOperation) argument.getAllValues().get(1)).getNodeName());
    master.shutdown();
  }

  private void checkStartStop(boolean shutdownClient, ZkServer zkServer) throws KattaException, InterruptedException,
          Exception {
    final Master master;
    if (zkServer != null) {
      master = new Master(_protocol, zkServer);
    } else {
      master = new Master(_protocol, shutdownClient);
    }

    MasterQueue masterQueue = mockBlockingOperationQueue();
    when(_protocol.publishMaster(master)).thenReturn(masterQueue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    TestUtil.waitUntilLeaveSafeMode(master);

    // stop agin
    master.shutdown();
    verify(_protocol).unregisterComponent(master);
    if (shutdownClient) {
      verify(_protocol).disconnect();
    }
    if (zkServer != null) {
      verify(zkServer).shutdown();
    }
  }

  private MasterQueue mockBlockingOperationQueue() throws InterruptedException {
    MasterQueue queue = mock(MasterQueue.class);
    when(queue.peek()).thenAnswer(new SleepingAnswer());
    return queue;
  }

}
