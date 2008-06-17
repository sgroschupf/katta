/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.contrib.dlucene;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;

public class MiniDLuceneCluster {
  private List<DataNode> dataNodes = null;
  private List<Integer> dataNodePorts = null;
  private NameNode nameNode = null;
  private int nameNodePort;

  public MiniDLuceneCluster(Configuration conf, int numberOfWorkers)
      throws Exception {
    dataNodes = new ArrayList<DataNode>();
    dataNodePorts = new ArrayList<Integer>();
    nameNodePort = TestUtils.getNextPort();
    InetSocketAddress nameNodeAddr = new InetSocketAddress(Constants.HOST, nameNodePort);
    final ZKClient zkclient = new ZKClient(new ZkConfiguration());
    nameNode = NameNode.createNode(zkclient, conf, nameNodeAddr);
    for (int i = 0; i < numberOfWorkers; i++) {
      int port = TestUtils.getNextPort();
      dataNodePorts.add(port);
      DataNode dn = DataNode.createNode(zkclient, conf,
          new InetSocketAddress(Constants.HOST, port), nameNodeAddr,
          TestConstants.USE_RAM_INDEX_FOR_TESTS);
      dataNodes.add(dn);
    }
  }

  public void shutdown() {
    for (DataNode dn : dataNodes) {
      dn.shutdown();
    }
    nameNode.shutdown();
  }

  /**
   * @return the nameNodePort
   */
  int getNameNodePort() {
    return nameNodePort;
  }

  public DataNode getDataNode(int i) {
    return dataNodes.get(i);
  }

  public int getDataNodePort(int i) {
    return dataNodePorts.get(i);
  }

  /**
   * @return the nameNode
   */
  public NameNode getNameNode() {
    return nameNode;
  }
}
