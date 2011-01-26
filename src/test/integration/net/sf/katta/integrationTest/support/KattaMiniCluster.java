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
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.master.Master;
import net.sf.katta.node.IContentServer;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
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

  private final Class<? extends IContentServer> _contentServerClass;
  private final ZkConfiguration _zkConfiguration;
  private Master _master;
  private Master _secondaryMaster;
  private List<Node> _nodes = new ArrayList<Node>();
  private ZkServer _zkServer;
  private InteractionProtocol _protocol;
  private int _nodeCount;
  private final int _nodeStartPort;
  private int _startedNodes;

  public KattaMiniCluster(ZkConfiguration zkConfiguration, int nodeCount) {
    this(LuceneServer.class, zkConfiguration, nodeCount, 20000);
  }

  public KattaMiniCluster(Class<? extends IContentServer> nodeServerClass, ZkConfiguration zkConfiguration,
          int nodeCount, int nodeStartPort) {
    _contentServerClass = nodeServerClass;
    _zkConfiguration = zkConfiguration;
    _nodeCount = nodeCount;
    _nodeStartPort = nodeStartPort;
  }

  public void setZkServer(ZkServer zkServer) {
    _zkServer = zkServer;
  }

  public void start() throws Exception {
    if (_zkServer == null) {
      _zkServer = ZkKattaUtil.startZkServer(_zkConfiguration);
    }
    _protocol = new InteractionProtocol(_zkServer.getZkClient(), _zkConfiguration);
    NodeConfiguration nodeConfiguration = new NodeConfiguration();
    nodeConfiguration.setStartPort(_nodeStartPort);
    for (int i = 0; i < _nodeCount; i++) {
      _nodes.add(new Node(_protocol, nodeConfiguration, _contentServerClass.newInstance()));
    }
    _master = new Master(_protocol, false);
    _master.start();
    for (Node node : _nodes) {
      node.start();
      _startedNodes++;
    }
    TestUtil.waitUntilLeaveSafeMode(_master);
    TestUtil.waitUntilNumberOfLiveNode(_protocol, _nodes.size());
  }

  public Node startAdditionalNode() throws Exception {
    NodeConfiguration nodeConfiguration = new NodeConfiguration();
    nodeConfiguration.setStartPort(_nodeStartPort + _startedNodes);
    Node node = new Node(_protocol, nodeConfiguration, _contentServerClass.newInstance());
    _nodes.add(node);
    node.start();
    _startedNodes++;
    return node;
  }

  public Master startSecondaryMaster() throws KattaException {
    _secondaryMaster = new Master(_protocol, false);
    _secondaryMaster.start();
    return _secondaryMaster;
  }

  public void restartMaster() throws Exception {
    _master.shutdown();
    _master = new Master(_protocol, false);
    _master.start();
    TestUtil.waitUntilLeaveSafeMode(_master);
  }

  public void stop() {
    stop(true);
  }

  public void stop(boolean stopZookeeper) {
    for (Node node : _nodes) {
      if (node.isRunning()) {
        node.shutdown();
      }
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    if (_secondaryMaster != null) {
      _secondaryMaster.shutdown();
    }
    _master.shutdown();
    if (stopZookeeper) {
      _zkServer.shutdown();
    }
  }

  public Node getNode(int i) {
    return _nodes.get(i);
  }

  public List<Node> getNodes() {
    return _nodes;
  }

  public int getRunningNodeCount() {
    return _nodes.size();
  }

  public int getStartedNodeCount() {
    return _startedNodes;
  }

  public Node shutdownNode(int i) {
    Node node = _nodes.remove(i);
    node.shutdown();
    return node;
  }

  public Node restartNode(int i) {
    Node shutdownNode = getNode(i);
    NodeConfiguration nodeConfiguration = new NodeConfiguration();
    nodeConfiguration.setStartPort(shutdownNode.getRPCServerPort());
    shutdownNode(i);
    Node node = new Node(_protocol, nodeConfiguration, shutdownNode.getContext().getContentServer());
    node.start();
    _nodes.add(i, node);
    return node;
  }

  public Node shutdownNodeRpc(int i) {
    Node node = _nodes.get(i);
    node.getRpcServer().stop();
    return node;
  }

  public Master getMaster() {
    return _master;
  }

  public List<String> deployTestIndexes(File indexFile, int deployCount, int replicationCount)
          throws InterruptedException {
    List<String> indices = new ArrayList<String>();
    ArrayList<IIndexDeployFuture> deployFutures = new ArrayList<IIndexDeployFuture>();
    IDeployClient deployClient = new DeployClient(_protocol);
    for (int i = 0; i < deployCount; i++) {
      String indexName = indexFile.getName() + i;
      IIndexDeployFuture deployFuture = deployClient.addIndex(indexName, indexFile.getAbsolutePath(), replicationCount);
      indices.add(indexName);
      deployFutures.add(deployFuture);
    }
    for (IIndexDeployFuture deployFuture : deployFutures) {
      deployFuture.joinDeployment();
    }
    return indices;
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
