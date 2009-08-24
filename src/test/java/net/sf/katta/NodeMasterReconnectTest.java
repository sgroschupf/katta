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
package net.sf.katta;

import java.util.concurrent.TimeUnit;

import net.sf.katta.master.Master;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.Gateway;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;

public class NodeMasterReconnectTest extends AbstractKattaTest {

  int GATEWAY_PORT = 2190;
  int ZK_SERVER_PORT = 2181;

  public void testReconnectNode() throws Exception {
    final ZkConfiguration gatewayConf = new ZkConfiguration();
    gatewayConf.setZKServers("localhost:" + GATEWAY_PORT);

    // startup the system
    Gateway gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    gateway.start();

    final MasterStartThread masterStartThread = startMaster();
    final Master master = masterStartThread.getMaster();
    final ZkClient zkNodeClient = ZkKattaUtil.startZkClient(gatewayConf, 30000);
    final Node node = new Node(gatewayConf, zkNodeClient, new LuceneServer());
    node.start();
    masterStartThread.join();
    final ZkClient zkMasterClient = masterStartThread.getZkClient();

    fail("Review the following two lines");
//    assertTrue(zkMasterClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));
//    assertTrue(zkNodeClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));

    // check node-master link
    waitOnNodes(masterStartThread, 1);
    assertTrue(master.getNodes().contains(node.getName()));

    // now break the node connection
    zkMasterClient.getEventLock().lock();
    try {
      gateway.interruptAndJoin();
      zkMasterClient.getEventLock().getDataChangedCondition().await(20, TimeUnit.SECONDS);
    } finally {
      zkMasterClient.getEventLock().unlock();
    }
    assertEquals(0, master.getNodes().size());

    // now fix the node connection
    gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    zkMasterClient.getEventLock().lock();
    gateway.start();
    zkMasterClient.getEventLock().getDataChangedCondition().await(20, TimeUnit.SECONDS);
    zkMasterClient.getEventLock().unlock();
    assertEquals(1, master.getNodes().size());

    node.shutdown();
    masterStartThread.shutdown();
    gateway.interruptAndJoin();
  }

}
