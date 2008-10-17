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
package net.sf.katta.integrationTest;

import java.io.File;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkServer;

/**
 * A container class for a whole katta cluster including:<br>
 * - zk server<br>
 * - master<br>
 * - nodes<br>
 */
public class KattaMiniCluster {

  private final ZkConfiguration _zkConfiguration;
  private ZkServer _zkServer;
  private final Master _master;
  private final Node[] _nodes;

  public KattaMiniCluster(ZkConfiguration zkConfiguration, int nodeCount) throws KattaException {
    _zkConfiguration = zkConfiguration;
    _nodes = new Node[nodeCount];
    for (int i = 0; i < _nodes.length; i++) {
      NodeConfiguration nodeConf = new NodeConfiguration();
      nodeConf.setShardFolder(new File(nodeConf.getShardFolder(), "" + i).getAbsolutePath());
      _nodes[i] = new Node(new ZKClient(_zkConfiguration), nodeConf);
    }
    _master = new Master(new ZKClient(zkConfiguration));
  }

  public void start() throws KattaException {
    _zkServer = new ZkServer(_zkConfiguration);
    _master.start();
    for (Node node : _nodes) {
      node.start();
    }
  }

  public void stop() {
    _master.shutdown();
    for (Node node : _nodes) {
      node.shutdown();
    }
    _zkServer.shutdown();
  }

  public Node getNode(int i) {
    return _nodes[i];
  }

  public void deployTestIndexes(File indexFile, Class<?> analyzerClass, int deployCount, int replicationCount)
      throws KattaException, InterruptedException {
    IDeployClient deployClient = new DeployClient(_zkConfiguration);
    for (int i = 0; i < deployCount; i++) {
      deployClient.addIndex(indexFile.getName() + i, indexFile.getAbsolutePath(), analyzerClass.getName(),
          replicationCount).joinDeployment();
    }
    deployClient.disconnect();
  }

}
