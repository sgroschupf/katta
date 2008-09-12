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
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class MasterTest extends AbstractKattaTest {

  private static final String SECOND_SHARD_FOLDER = "/tmp/katta-shards2";

  @Override
  protected void onSetUp2() throws Exception {
    org.apache.hadoop.fs.FileUtil.fullyDelete(new File(SECOND_SHARD_FOLDER));
  }

  public void testNodes() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();
    ZKClient zkClient = new ZKClient(_conf);
    zkClient.start(5000);
    Master master = masterStartThread.getMaster();

    String node1 = "node1";
    String node2 = "node2";
    zkClient.createEphemeral(ZkPathes.getNodePath(node1), new NodeMetaData(node1, NodeState.IN_SERVICE));
    zkClient.create(ZkPathes.getNode2ShardRootPath(node1));
    zkClient.createEphemeral(ZkPathes.getNodePath(node2), new NodeMetaData(node2, NodeState.IN_SERVICE));
    zkClient.create(ZkPathes.getNode2ShardRootPath(node2));

    masterStartThread.join();
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);
    assertEquals(2, master.readNodes().size());
    zkClientMaster.getEventLock().lock();
    assertTrue(zkClientMaster.delete("/katta/nodes/node1"));
    zkClientMaster.getEventLock().getDataChangedCondition().await();
    zkClientMaster.getEventLock().unlock();

    assertEquals(1, master.readNodes().size());
    zkClient.close();
    masterStartThread.shutdown();
  }

  public void testNodesReconnect() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClient = new ZKClient(_conf);
    zkClient.start(5000);
    Master master = masterStartThread.getMaster();
    ZKClient masterZkClient = masterStartThread.getZkClient();

    String nodePath = ZkPathes.getNodePath("node1");
    zkClient.create(ZkPathes.getNode2ShardRootPath("node1"));
    zkClient.create(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));

    masterStartThread.join();
    waitForChilds(zkClient, ZkPathes.NODES, 1);
    assertEquals(1, master.readNodes().size());

    // disconnect
    masterZkClient.getEventLock().lock();
    assertTrue(zkClient.delete(nodePath));
    masterZkClient.getEventLock().getDataChangedCondition().await();
    masterZkClient.getEventLock().unlock();
    assertEquals(0, master.readNodes().size());

    // reconnect
    masterZkClient.getEventLock().lock();
    zkClient.create(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));
    masterZkClient.getEventLock().getDataChangedCondition().await();
    masterZkClient.getEventLock().unlock();

    assertEquals(1, master.readNodes().size());
    zkClient.close();
    masterStartThread.shutdown();
  }

  public void testDeployAndRemoveIndex() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();

    NodeStartThread nodeStartThread1 = startNode();
    NodeStartThread nodeStartThread2 = startNode(SECOND_SHARD_FOLDER);
    Node node1 = nodeStartThread1.getNode();
    Node node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();

    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = TestResources.INDEX1;
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

    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }

  public void testRebalanceIndexAfterNodeCrash() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();

    NodeStartThread nodeStartThread1 = startNode();
    NodeStartThread nodeStartThread2 = startNode(SECOND_SHARD_FOLDER);
    Node node1 = nodeStartThread1.getNode();
    Node node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = TestResources.INDEX1;
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
    } while (indexState != IndexState.DEPLOYED || masterStartThread.getMaster().getNodes().size() > 1);

    nodeStartThread1.shutdown();
    masterStartThread.shutdown();
  }

  public void testDeployError() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();

    NodeStartThread nodeStartThread1 = startNode();
    NodeStartThread nodeStartThread2 = startNode(SECOND_SHARD_FOLDER);
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 2);

    final File indexFile = TestResources.INVALID_INDEX;
    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 2);

    final IndexMetaData metaData = new IndexMetaData();
    zkClientMaster.readData(ZkPathes.getIndexPath(index), metaData);
    assertEquals(IndexMetaData.IndexState.ERROR, metaData.getState());

    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }

  public void testIndexPickupAfterMasterRestart() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();

    NodeStartThread nodeStartThread = startNode();
    masterStartThread.join();
    nodeStartThread.join();
    waitForPath(zkClientMaster, ZkPathes.MASTER);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 1);

    // add index
    final File indexFile = TestResources.INDEX1;
    int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;

    final Katta katta = new Katta();
    String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), StandardAnalyzer.class.getName(), 2);
    assertEquals(shardCount, zkClientMaster.countChildren(ZkPathes.getIndexPath(index)));

    // restartmaster
    masterStartThread.shutdown();
    masterStartThread = startMaster();
    masterStartThread.join();
    assertEquals(1, masterStartThread.getMaster().getIndexes().size());

    nodeStartThread.shutdown();
    masterStartThread.shutdown();
  }

  public void testReplicateUnderreplicatedIndexesAfterNodeAdding() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZKClient zkClientMaster = masterStartThread.getZkClient();
    ZKClient zkClient = new ZKClient(_conf);
    zkClient.start(5000);
    Master master = masterStartThread.getMaster();

    // start one node
    NodeStartThread nodeStartThread1 = startNode();
    masterStartThread.join();
    assertEquals(1, master.readNodes().size());

    // add index with replication level of 2
    final File indexFile = TestResources.INDEX1;
    String index = "indexA";
    DeployClient deployClient = new DeployClient(zkClient);
    IIndexDeployFuture deployFuture = deployClient.addIndex(index, "file://" + indexFile.getAbsolutePath(),
        StandardAnalyzer.class.getName(), 2);
    deployFuture.joinDeployment();
    assertEquals(1, deployClient.getIndexes(IndexState.DEPLOYED).size());
    List<String> shards = zkClient.getChildren(ZkPathes.getIndexPath(index));
    for (String shard : shards) {
      assertEquals(1, zkClient.countChildren(ZkPathes.getShard2NodeRootPath(shard)));
    }

    // start node2
    zkClientMaster.getEventLock().lock();
    NodeStartThread nodeStartThread2 = startNode(SECOND_SHARD_FOLDER);
    zkClientMaster.getEventLock().getDataChangedCondition().await();
    zkClientMaster.getEventLock().unlock();
    assertEquals(2, master.readNodes().size());

    // replication should now take place
    for (String shard : shards) {
      waitForChilds(zkClient, ZkPathes.getShard2NodeRootPath(shard), 2);
    }

    deployClient.disconnect();
    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }
}
