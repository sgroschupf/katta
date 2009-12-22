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

import static org.junit.Assert.assertEquals;
import net.sf.katta.master.Master;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

public class NodeMasterReconnectTest extends AbstractZkTest {

  private static final int GATEWAY_PORT = 2190;

  @Test(timeout = 50000)
  public void testReconnectNode() throws Exception {
    final Master master = new Master(_zk.getInteractionProtocol(), false);

    // startup node over gateway
    final ZkConfiguration gatewayConf = new ZkConfiguration();
    gatewayConf.setZKRootPath(_zk.getZkConf().getZKRootPath());
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
    TestUtil.waitOnLeaveSafeMode(master);
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
