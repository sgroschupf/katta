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
package net.sf.katta.integrationTest.support;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.master.Master;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

/**
 * A container class for a whole katta cluster including:<br>
 * - zk server<br>
 * - master<br>
 * - nodes<br>
 */
public class KattaMiniCluster {

  private final ZkConfiguration _zkConfiguration;
  private Master _master;
  private List<Node> _nodes = new ArrayList<Node>();
  private ZkServer _zkServer;
  private InteractionProtocol _protocol;
  private int _nodeCount;

  public KattaMiniCluster(ZkConfiguration zkConfiguration, int nodeCount) {
    _zkConfiguration = zkConfiguration;
    _nodeCount = nodeCount;
  }

  public void start() throws Exception {
    _zkServer = ZkKattaUtil.startZkServer(_zkConfiguration);
    _protocol = new InteractionProtocol(_zkServer.getZkClient(), _zkConfiguration);
    for (int i = 0; i < _nodeCount; i++) {
      _nodes.add(new Node(_protocol, new LuceneServer()));
    }
    _master = new Master(_protocol, false);
    _master.start();
    for (Node node : _nodes) {
      node.start();
    }
    TestUtil.waitOnLeaveSafeMode(_master);
  }

  public void startAdditionalNode() {
    Node node = new Node(_protocol, new LuceneServer());
    _nodes.add(node);
    node.start();
  }

  public void restartMaster() throws Exception {
    _master.shutdown();
    _master = new Master(_protocol, false);
    _master.start();
    TestUtil.waitOnLeaveSafeMode(_master);
  }

  public void stop() {
    for (Node node : _nodes) {
      node.shutdown();
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    _master.shutdown();
    _zkServer.shutdown();
  }

  public Node getNode(int i) {
    return _nodes.get(i);
  }

  public void deployTestIndexes(File indexFile, int deployCount, int replicationCount) throws InterruptedException {
    IDeployClient deployClient = new DeployClient(_zkServer.getZkClient(), _zkConfiguration);
    for (int i = 0; i < deployCount; i++) {
      deployClient.addIndex(indexFile.getName() + i, indexFile.getAbsolutePath(), replicationCount).joinDeployment();
    }
  }

  public ZkConfiguration getZkConfiguration() {
    return _zkConfiguration;
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  public ZkServer getZkServer() {
    return _zkServer;
  }

  public ZkClient getZkClient() {
    return _zkServer.getZkClient();
  }

}
