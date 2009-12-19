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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.testutil.mockito.SleepingAnswer;
import net.sf.katta.util.KattaException;

import org.I0Itec.zkclient.ZkServer;
import org.junit.Test;

public class MasterMockTest {

  @Test
  public void testBecomeMaster() throws Exception {
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    final Master master = new Master(protocol, false);

    OperationQueue masterQueue = mockBlockingOperationQueue();
    when(protocol.publishMaster(master)).thenReturn(masterQueue);
    when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    assertTrue(master.isMaster());
    master.shutdown();
  }

  @Test
  public void testBecomeSecMaster() throws Exception {
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    final Master master = new Master(protocol, false);

    when(protocol.publishMaster(master)).thenReturn(null);
    when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    assertFalse(master.isMaster());
    master.shutdown();
  }

  @Test
  public void testDisconnectReconnect() throws Exception {
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    final Master master = new Master(protocol, false);

    OperationQueue masterQueue = mockBlockingOperationQueue();
    when(protocol.publishMaster(master)).thenReturn(masterQueue);
    when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

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

  // TODO test disconnect ?
  private void checkStartStop(boolean shutdownClient, ZkServer zkServer) throws KattaException, InterruptedException,
          Exception {
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    final Master master;
    if (zkServer != null) {
      master = new Master(protocol, zkServer);
    } else {
      master = new Master(protocol, shutdownClient);
    }

    OperationQueue masterQueue = mockBlockingOperationQueue();
    when(protocol.publishMaster(master)).thenReturn(masterQueue);
    when(protocol.getLiveNodes()).thenReturn(Arrays.asList("node1"));

    master.start();
    TestUtil.waitOnLeaveSafeMode(master);

    // stop agin
    master.shutdown();
    verify(protocol).unregisterComponent(master);
    if (shutdownClient) {
      verify(protocol).disconnect();
    }
    if (zkServer != null) {
      verify(zkServer).shutdown();
    }
  }

  private OperationQueue mockBlockingOperationQueue() throws InterruptedException {
    OperationQueue queue = mock(OperationQueue.class);
    when(queue.peek()).thenAnswer(new SleepingAnswer());
    return queue;
  }

}
