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

import java.util.List;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.operation.master.IndexDeployOperation;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.master.MasterOperation.ExecutionInstruction;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.testutil.Mocks;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.testutil.mockito.SerializableCountDownLatchAnswer;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import static org.mockito.Matchers.notNull;

public class MasterZkTest extends AbstractZkTest {

  @Test
  public void testShutdown_shouldCleanupZkClientSubscriptions() throws KattaException {
    int numberOfListeners = _zk.getZkClient().numberOfListeners();
    Master master = new Master(_zk.getInteractionProtocol(), false);
    master.start();
    master.shutdown();
    assertEquals(numberOfListeners, _zk.getZkClient().numberOfListeners());
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 10000)
  public void testMasterOperationPickup() throws Exception {
    Master master = new Master(_zk.getInteractionProtocol(), false);
    Node node = Mocks.mockNode();// leave safe mode
    _protocol.publishNode(node, new NodeMetaData("node1"));
    master.start();

    MasterOperation operation1 = mock(MasterOperation.class, withSettings().serializable());
    MasterOperation operation2 = mock(MasterOperation.class, withSettings().serializable());

    SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);
    when(operation1.getExecutionInstruction((List<MasterOperation>) notNull()))
            .thenReturn(ExecutionInstruction.EXECUTE);
    when(operation2.getExecutionInstruction((List<MasterOperation>) notNull()))
            .thenReturn(ExecutionInstruction.EXECUTE);
    when(operation1.execute((MasterContext) notNull(), (List<MasterOperation>) notNull())).thenAnswer(answer);
    when(operation2.execute((MasterContext) notNull(), (List<MasterOperation>) notNull())).thenAnswer(answer);
    _protocol.addMasterOperation(operation1);
    _protocol.addMasterOperation(operation2);
    answer.getCountDownLatch().await();

    master.shutdown();
  }

  @Test(timeout = 500000)
  public void testMasterChange_OnSessionReconnect() throws Exception {
    Master master = new Master(_zk.getInteractionProtocol(), false);
    Node node = Mocks.mockNode();// leave safe mode
    _protocol.publishNode(node, new NodeMetaData("node1"));
    master.start();
    TestUtil.waitUntilLeaveSafeMode(master);

    Master secMaster = new Master(_zk.getInteractionProtocol(), false);
    secMaster.start();

    master.disconnect();
    master.reconnect();
    master.shutdown();
    secMaster.shutdown();
  }

  @Test(timeout = 50000)
  public void testMasterChangeWhileDeploingIndex() throws Exception {
    Master master = new Master(_zk.getInteractionProtocol(), false);
    Node node = Mocks.mockNode();// leave safe mode
    NodeQueue nodeQueue = _protocol.publishNode(node, new NodeMetaData("node1"));
    master.start();
    TestUtil.waitUntilLeaveSafeMode(master);

    // phase I - until watchdog is running and its node turn
    IndexDeployOperation deployOperation = new IndexDeployOperation("index1", TestResources.INDEX1.getAbsolutePath(), 1);
    _protocol.addMasterOperation(deployOperation);
    while (!master.getContext().getMasterQueue().isEmpty()) {
      // wait until deploy is in watch phase
      Thread.sleep(100);
    }

    // phase II - master change while node is deploying
    master.shutdown();
    Master secMaster = new Master(_zk.getInteractionProtocol(), false);
    secMaster.start();

    // phase III - finish node operations/ mater operation should be finished
    while (!nodeQueue.isEmpty()) {
      nodeQueue.remove();
    }
    TestUtil.waitUntilIndexDeployed(_protocol, deployOperation.getIndexName());
    assertNotNull(_protocol.getIndexMD(deployOperation.getIndexName()));

    secMaster.shutdown();
  }

  @Test(timeout = 50000)
  public void testReconnectNode() throws Exception {
    final int GATEWAY_PORT = 2190;
    final Master master = new Master(_zk.getInteractionProtocol(), false);

    // startup node over gateway
    final ZkConfiguration gatewayConf = new ZkConfiguration();
    gatewayConf.setZKRootPath(_zk.getZkConf().getZkRootPath());
    gatewayConf.setZKServers("localhost:" + GATEWAY_PORT);
    Gateway gateway = new Gateway(GATEWAY_PORT, _zk.getServerPort());
    gateway.start();
    final ZkClient zkGatewayClient = ZkKattaUtil.startZkClient(gatewayConf, 30000);
    InteractionProtocol gatewayProtocol = new InteractionProtocol(zkGatewayClient, gatewayConf);
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());
    final Node node = new Node(gatewayProtocol, new LuceneServer());
    node.start();

    // check node-master link
    master.start();
    TestUtil.waitUntilLeaveSafeMode(master);
    TestUtil.waitUntilNumberOfLiveNode(_protocol, 1);
    assertEquals(1, _protocol.getLiveNodes().size());

    // now break the node connection
    gateway.stop();
    TestUtil.waitUntilNumberOfLiveNode(_protocol, 0);

    // now fix the node connection
    gateway.start();
    TestUtil.waitUntilNumberOfLiveNode(_protocol, 1);

    // cleanup
    node.shutdown();
    master.shutdown();
    zkGatewayClient.close();
    gateway.stop();
  }

}
