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
import net.sf.katta.TimingTestUtil;
import net.sf.katta.ZkServer;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.NodeTest;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class MasterTest extends AbstractKattaTest {

  /*
   * create /katta/nodes folder, subscribe notification handle addIndex by
   * distibute shards to nodes handle node failure
   */
  public void testNodes() throws Exception {
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    client.start(5000);

    final Master master = new Master(client);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    client.createEphemeral(IPaths.NODES + "/node1", new NodeMetaData("node1", "OK", true, 1));
    client.create(IPaths.NODE_TO_SHARD + "/node1");
    client.createEphemeral(IPaths.NODES + "/node2", new NodeMetaData("node2", "OK", true, 2));
    client.create(IPaths.NODE_TO_SHARD + "/node2");

    masterThread.join();
    assertEquals(2, master.readNodes().size());
    assertTrue(client.delete("/katta/nodes/node1"));

    assertEquals(1, master.readNodes().size());
    client.close();
    zkServer.shutdown();
    Thread.sleep(2000);
  }

  public void testDeployAndRemoveIndex() throws Exception {
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    final ZKClient client2 = new ZKClient(conf);
    client.start(5000);

    final Master master = new Master(client);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();
    TimingTestUtil.waitFor(client, IPaths.MASTER);

    Node node1 = NodeTest.startNodeServer(client);
    Node node2 = NodeTest.startNodeServer(client2, "/tmp/katta-shards2");
    masterThread.join();
    TimingTestUtil.waitFor(client, IPaths.NODES, 2);

    final File file = new File("./src/test/testIndexA");
    final String path = "file://" + file.getAbsolutePath();
    final Katta katta = new Katta();
    katta.addIndex("indexA", path, StandardAnalyzer.class.getName(), 2);

    // there should be two servers here now.
    assertEquals(2, client.getChildren(IPaths.NODE_TO_SHARD).size());

    // there should be two shards here now
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getNode()).size());

    // there should be two shards here now
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/" + node2.getNode()).size());
    // there should be 4 shards indexes now..
    List<String> shardsToNode = client.getChildren(IPaths.SHARD_TO_NODE);
    assertEquals(4, shardsToNode.size());

    // now we fake the nodes and add the nodes to the shards
    shardsToNode = client.getChildren(IPaths.SHARD_TO_NODE);
    assertEquals(4, shardsToNode.size());

    final IndexMetaData metaData = new IndexMetaData();
    client.readData(IPaths.INDEXES + "/indexA", metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());

    synchronized (client.getSyncMutex()) {
      node2.shutdown();
      // the node event in the master NodeListerner should notify
      client.getSyncMutex().wait();
    }
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getNode()).size());

    List<String> shardsToIndex = client.getChildren(IPaths.INDEXES + "/indexA");
    assertEquals(4, shardsToIndex.size());

    katta.removeIndex("indexA");
    int count = 0;
    while (client.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getNode()).size() != 0) {
      Thread.sleep(500);
      if (count++ > 40) {
        fail("shards are still not removed from node after 20 sec.");
      }
    }
    assertEquals(0, client.getChildren(IPaths.NODE_TO_SHARD + "/" + node1.getNode()).size());

    node1.shutdown();
    client.close();
    zkServer.shutdown();
  }

  public void testDeployError() throws Exception {
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    final ZKClient client2 = new ZKClient(conf);
    client.start(5000);

    final Master master = new Master(client);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();
    TimingTestUtil.waitFor(client, IPaths.MASTER);

    NodeTest.startNodeServer(client);
    NodeTest.startNodeServer(client2);
    masterThread.join();
    TimingTestUtil.waitFor(client, IPaths.NODES, 2);

    final File file = new File("./src/test/testIndexInvalid");
    final String path = "file://" + file.getAbsolutePath();
    final Katta katta = new Katta();
    katta.addIndex("indexA", path, StandardAnalyzer.class.getName(), 2);

    final IndexMetaData metaData = new IndexMetaData();
    client.readData(IPaths.INDEXES + "/indexA", metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOY_ERROR, metaData.getState());

    client2.close();
    client.close();
    zkServer.shutdown();
  }
}
