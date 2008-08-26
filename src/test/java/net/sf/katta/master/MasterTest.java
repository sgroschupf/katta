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
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.util.FileUtil;
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

    String node1 = "node1";
    String node2 = "node2";
    zkClientMaster.createEphemeral(ZkPathes.getNodePath(node1), new NodeMetaData(node1, NodeState.IN_SERVICE));
    zkClientMaster.create(ZkPathes.getNode2ShardRootPath(node1));
    zkClientMaster.createEphemeral(ZkPathes.getNodePath(node2), new NodeMetaData(node2, NodeState.IN_SERVICE));
    zkClientMaster.create(ZkPathes.getNode2ShardRootPath(node2));

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
    zkClientMaster.create(ZkPathes.getNode2ShardRootPath("node1"));
    zkClientMaster.create(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));

    masterThread.join();
    assertEquals(1, master.readNodes().size());

    // disconnect
    assertTrue(zkClientMaster.delete(nodePath));
    assertEquals(0, master.readNodes().size());

    // reconnect
    synchronized (zkClientMaster.getSyncMutex()) {
      zkClientMaster.create(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));
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
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = new File("src/test/testIndexA");
    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 2);

    int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getIndexPath(index)));
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getNode2ShardRootPath(node1.getName())));
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getNode2ShardRootPath(node2.getName())));

    List<String> shards = zkClientMaster.getChildren(ZkPathes.SHARD_TO_NODE);
    assertEquals(shardCount, shards.size());
    for (String shard : shards) {
      // each shard should be on both nodes
      assertEquals(2, zkClientMaster.getChildren(ZkPathes.getShard2NodeRootPath(shard)).size());
    }

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(ZkPathes.getIndexPath(index), metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());

    katta.removeIndex(index);
    int count = 0;
    while (zkClientMaster.getChildren(ZkPathes.getNode2ShardRootPath(node1.getName())).size() != 0) {
      Thread.sleep(500);
      if (count++ > 40) {
        fail("shards are still not removed from node after 20 sec.");
      }
    }
    assertEquals(0, zkClientMaster.getChildren(ZkPathes.getNode2ShardRootPath(node1.getName())).size());

    node1.shutdown();
    node2.shutdown();
    master.shutdown();
  }

  public void testRebalanceIndexAfterNodeCrash() throws Exception {
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
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = new File("src/test/testIndexA");
    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 1);

    int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getIndexPath(index)));
    assertEquals(shardCount / 2, zkClientMaster.countChildren(ZkPathes.getNode2ShardRootPath(node1.getName())));
    assertEquals(shardCount / 2, zkClientMaster.countChildren(ZkPathes.getNode2ShardRootPath(node2.getName())));

    List<String> shards = zkClientMaster.getChildren(ZkPathes.SHARD_TO_NODE);
    assertEquals(shardCount, shards.size());
    for (String shard : shards) {
      // each shard should be on one nodes
      assertEquals(1, zkClientMaster.getChildren(ZkPathes.getShard2NodeRootPath(shard)).size());
    }

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(ZkPathes.getIndexPath(index), metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());
    node2.shutdown();

    long time = System.currentTimeMillis();
    IndexState indexState;
    do {
      zkClientMaster.readData(ZkPathes.getIndexPath(index), metaData);
      indexState = metaData.getState();
      if (System.currentTimeMillis() - time > 1000 * 60) {
        fail("index is not in deployed state again");
      }
    } while (indexState != IndexState.DEPLOYED || master.getNodes().size() > 1);

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
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = new File("src/test/testIndexInvalid");
    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 2);

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(ZkPathes.getIndexPath(index), metaData);
    assertEquals(IndexMetaData.IndexState.ERROR, metaData.getState());

    node1.shutdown();
    node2.shutdown();
    master.shutdown();
  }

  public void testIndexPickupAfterMasterRestart() throws Exception {
    createZkServer();
    final ZKClient zkClientMaster = new ZKClient(conf);
    final ZKClient zkClientNode1 = new ZKClient(conf);

    Master master = new Master(zkClientMaster);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    Node node1 = startNodeServer(zkClientNode1);
    masterThread.join();
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 1);

    // add index
    final File indexFile = new File("src/test/testIndexA");
    int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;

    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 2);
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getIndexPath(index)));

    // restartmaster
    master.shutdown();
    master = new Master(zkClientMaster);
    masterThread = createStartMasterThread(master);
    masterThread.start();
    masterThread.join();
    assertEquals(1, master.getIndexes().size());

    node1.shutdown();
    master.shutdown();
  }
}
