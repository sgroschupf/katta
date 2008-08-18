/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package net.sf.katta.master;

import java.io.File;
import java.util.List;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class MasterTest extends AbstractKattaTest {

  /*
   * create /katta/nodes folder, subscribe notification handle addIndex by
   * distibute shards to nodes handle node failure
   */
  public void testNodes() throws Exception {
    createZkServer();
    final ZKClient zkClientMaster = new ZKClient(conf);
    zkClientMaster.start(5000);

    final Master master = new Master(zkClientMaster);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    zkClientMaster.createEphemeral(IPaths.NODES + "/node1", new NodeMetaData("node1", "OK", true, 1));
    zkClientMaster.create(IPaths.NODE_TO_SHARD + "/node1");
    zkClientMaster.createEphemeral(IPaths.NODES + "/node2", new NodeMetaData("node2", "OK", true, 2));
    zkClientMaster.create(IPaths.NODE_TO_SHARD + "/node2");

    masterThread.join();
    assertEquals(2, master.readNodes().size());
    assertTrue(zkClientMaster.delete("/katta/nodes/node1"));

    assertEquals(1, master.readNodes().size());
    master.shutdown();
  }

  public void testNodesReconnect() throws Exception {
    createZkServer();
    final ZKClient zkClientMaster = new ZKClient(conf);
    zkClientMaster.start(5000);

    final Master master = new Master(zkClientMaster);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    String nodePath = ZkPathes.getNodePath("node1");
    zkClientMaster.create(nodePath, new NodeMetaData("node1", "OK", true, 1));

    masterThread.join();
    assertEquals(1, master.readNodes().size());

    // disconnect
    assertTrue(zkClientMaster.delete(nodePath));
    assertEquals(0, master.readNodes().size());

    // reconnect
    synchronized (zkClientMaster.getSyncMutex()) {
      zkClientMaster.create(nodePath, new NodeMetaData("node1", "OK", true, 1));
      zkClientMaster.getSyncMutex().wait();
    }
    assertEquals(1, master.readNodes().size());
    master.shutdown();
  }

  public void testDeployAndRemoveIndex() throws Exception {
    createZkServer();
    final ZKClient zkClientMaster = new ZKClient(conf);
    final ZKClient zkClientNode1 = new ZKClient(conf);
    final ZKClient zkClientNode2 = new ZKClient(conf);
    zkClientMaster.start(5000);

    final Master master = new Master(zkClientMaster);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    Node node1 = startNodeServer(zkClientNode1);
    Node node2 = startNodeServer(zkClientNode2, "/tmp/katta-shards2");
    masterThread.join();
    waitForPath(zkClientMaster, IPaths.MASTER);
    waitForChilds(zkClientMaster, IPaths.NODES, 2);

    final File file = new File("./src/test/testIndexA");
    final String path = "file://" + file.getAbsolutePath();
    final Katta katta = new Katta();
    katta.addIndex("indexA", path, StandardAnalyzer.class.getName(), 2);

    // there should be two servers here now.
    assertEquals(2, zkClientMaster.getChildren(IPaths.NODE_TO_SHARD).size());

    // there should be two shards here now
    assertEquals(4, zkClientMaster.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getName()).size());

    // there should be two shards here now
    assertEquals(4, zkClientMaster.getChildren(IPaths.NODE_TO_SHARD + "/" + node2.getName()).size());
    // there should be 4 shards indexes now..
    List<String> shardsToNode = zkClientMaster.getChildren(IPaths.SHARD_TO_NODE);
    assertEquals(4, shardsToNode.size());

    // now we fake the nodes and add the nodes to the shards
    shardsToNode = zkClientMaster.getChildren(IPaths.SHARD_TO_NODE);
    assertEquals(4, shardsToNode.size());

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(IPaths.INDEXES + "/indexA", metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());

    synchronized (zkClientMaster.getSyncMutex()) {
      node2.shutdown();
      // the node event in the master NodeListerner should notify
      zkClientMaster.getSyncMutex().wait();
    }
    assertEquals(4, zkClientMaster.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getName()).size());

    List<String> shardsToIndex = zkClientMaster.getChildren(IPaths.INDEXES + "/indexA");
    assertEquals(4, shardsToIndex.size());

    katta.removeIndex("indexA");
    int count = 0;
    while (zkClientMaster.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getName()).size() != 0) {
      Thread.sleep(500);
      if (count++ > 40) {
        fail("shards are still not removed from node after 20 sec.");
      }
    }
    assertEquals(0, zkClientMaster.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getName()).size());

    node1.shutdown();
    master.shutdown();
  }

  public void testDeployError() throws Exception {
    createZkServer();
    final ZKClient zkClientMaster = new ZKClient(conf);
    final ZKClient zkClientNode1 = new ZKClient(conf);
    final ZKClient zkClientNode2 = new ZKClient(conf);

    final Master master = new Master(zkClientMaster);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    Node node1 = startNodeServer(zkClientNode1);
    Node node2 = startNodeServer(zkClientNode2);
    masterThread.join();
    waitForPath(zkClientMaster, IPaths.MASTER);
    waitForChilds(zkClientMaster, IPaths.NODES, 2);

    final File file = new File("./src/test/testIndexInvalid");
    final String path = "file://" + file.getAbsolutePath();
    final Katta katta = new Katta();
    katta.addIndex("indexA", path, StandardAnalyzer.class.getName(), 2);

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(IPaths.INDEXES + "/indexA", metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOY_ERROR, metaData.getState());

    node1.shutdown();
    node2.shutdown();
    master.shutdown();
  }
}
