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
package net.sf.katta.master;

import java.io.File;
import java.util.List;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;

public class MasterTest extends AbstractKattaTest {

  private static final String SECOND_SHARD_FOLDER = "/tmp/katta-shards2";

  @Override
  protected void onSetUp2() throws Exception {
    org.apache.hadoop.fs.FileUtil.fullyDelete(new File(SECOND_SHARD_FOLDER));
  }

  public void testNodes() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();
    final ZkClient zkClient = new ZkClient(_conf.getZKServers(), _conf.getZKTimeOut(), 5000);
    final Master master = masterStartThread.getMaster();

    final String node1 = "node1";
    final String node2 = "node2";
    zkClient.createEphemeral(_conf.getZKNodePath(node1), new NodeMetaData(node1, NodeState.IN_SERVICE));
    zkClient.createPersistent(_conf.getZKNodeToShardPath(node1));
    zkClient.createEphemeral(_conf.getZKNodePath(node2), new NodeMetaData(node2, NodeState.IN_SERVICE));
    zkClient.createPersistent(_conf.getZKNodeToShardPath(node2));

    masterStartThread.join();
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 2);
    assertEquals(2, master.readNodes().size());
    zkClientMaster.getEventLock().lock();
    try {
      assertTrue(zkClientMaster.delete(_conf.getZKNodesPath() + "/node1"));
      zkClientMaster.getEventLock().getDataChangedCondition().await();
    } finally {
      zkClientMaster.getEventLock().unlock();
    }
    assertEquals(1, master.readNodes().size());
    zkClient.close();
    masterStartThread.shutdown();
  }

  public void testNodesReconnect() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClient = ZkKattaUtil.startZkClient(_conf, 5000);
    final Master master = masterStartThread.getMaster();
    final ZkClient masterZkClient = masterStartThread.getZkClient();

    final String nodePath = _conf.getZKNodePath("node1");
    zkClient.createPersistent(_conf.getZKNodeToShardPath("node1"));
    zkClient.createPersistent(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));

    masterStartThread.join();
    waitForChilds(zkClient, _conf.getZKNodesPath(), 1);
    assertEquals(1, master.readNodes().size());

    // disconnect
    masterZkClient.getEventLock().lock();
    try {
      assertTrue(zkClient.delete(nodePath));
      masterZkClient.getEventLock().getDataChangedCondition().await();
    } finally {
      masterZkClient.getEventLock().unlock();
    }
    assertEquals(0, master.readNodes().size());

    // reconnect
    masterZkClient.getEventLock().lock();
    zkClient.createPersistent(nodePath, new NodeMetaData("node1", NodeState.IN_SERVICE));
    masterZkClient.getEventLock().getDataChangedCondition().await();
    masterZkClient.getEventLock().unlock();

    assertEquals(1, master.readNodes().size());
    zkClient.close();
    masterStartThread.shutdown();
  }

  public void testDeployAndRemoveIndex() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();

    final NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    final NodeStartThread nodeStartThread2 = startNode(new LuceneServer(), SECOND_SHARD_FOLDER);
    final Node node1 = nodeStartThread1.getNode();
    final Node node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();

    waitForPath(zkClientMaster, _conf.getZKMasterPath());
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 2);

    final File indexFile = TestResources.INDEX1;
    final Katta katta = new Katta(_conf);
    final String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), 2);

    final int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;
    assertEquals(shardCount, zkClientMaster.countChildren(_conf.getZKIndexPath(index)));
    assertEquals(shardCount, zkClientMaster.countChildren(_conf.getZKNodeToShardPath(node1.getName())));
    assertEquals(shardCount, zkClientMaster.countChildren(_conf.getZKNodeToShardPath(node2.getName())));

    final List<String> shards = zkClientMaster.getChildren(_conf.getZKShardToNodePath());
    assertEquals(shardCount, shards.size());
    for (final String shard : shards) {
      // each shard should be on both nodes
      assertEquals(2, zkClientMaster.getChildren(_conf.getZKShardToNodePath(shard)).size());
    }

    final IndexMetaData metaData = zkClientMaster.readData(_conf.getZKIndexPath(index));
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());

    katta.removeIndex(index);
    int count = 0;
    while (zkClientMaster.getChildren(_conf.getZKNodeToShardPath(node1.getName())).size() != 0) {
      Thread.sleep(500);
      if (count++ > 40) {
        fail("shards are still not removed from node after 20 sec.");
      }
    }
    assertEquals(0, zkClientMaster.getChildren(_conf.getZKNodeToShardPath(node1.getName())).size());

    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }

  public void testRebalanceIndexAfterNodeCrash() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();

    final NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    final NodeStartThread nodeStartThread2 = startNode(new LuceneServer(), SECOND_SHARD_FOLDER);
    final Node node1 = nodeStartThread1.getNode();
    final Node node2 = nodeStartThread2.getNode();
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitForPath(zkClientMaster, _conf.getZKMasterPath());
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 2);

    final File indexFile = TestResources.INDEX1;
    DeployClient deployClient = new DeployClient(zkClientMaster, _conf);
    final String index = "indexA";
    IIndexDeployFuture deployFuture = deployClient.addIndex(index, "file://" + indexFile.getAbsolutePath(), 1);
    deployFuture.joinDeployment();

    final int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;
    assertEquals(shardCount, zkClientMaster.countChildren(_conf.getZKIndexPath(index)));
    assertEquals(shardCount / 2, zkClientMaster.countChildren(_conf.getZKNodeToShardPath(node1.getName())));
    assertEquals(shardCount / 2, zkClientMaster.countChildren(_conf.getZKNodeToShardPath(node2.getName())));

    final List<String> shards = zkClientMaster.getChildren(_conf.getZKShardToNodePath());
    assertEquals(shardCount, shards.size());
    for (final String shard : shards) {
      // each shard should be on one nodes
      assertEquals(1, zkClientMaster.getChildren(_conf.getZKShardToNodePath(shard)).size());
    }

    IndexMetaData metaData = zkClientMaster.readData(_conf.getZKIndexPath(index));
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());
    node2.shutdown();

    final long time = System.currentTimeMillis();
    IndexState indexState;
    do {
      metaData = zkClientMaster.readData(_conf.getZKIndexPath(index));
      indexState = metaData.getState();
      if (System.currentTimeMillis() - time > 1000 * 60) {
        fail("index is not in deployed state again");
      }
    } while (indexState != IndexState.DEPLOYED || masterStartThread.getMaster().getNodes().size() > 1);

    deployClient.disconnect();
    nodeStartThread1.shutdown();
    masterStartThread.shutdown();
  }

  public void testDeployError() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();

    final NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    final NodeStartThread nodeStartThread2 = startNode(new LuceneServer(), SECOND_SHARD_FOLDER);
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitForPath(zkClientMaster, _conf.getZKMasterPath());
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 2);

    final File indexFile = TestResources.INVALID_INDEX;
    final Katta katta = new Katta(_conf);
    final String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), 2);

    final IndexMetaData metaData = zkClientMaster.readData(_conf.getZKIndexPath(index));
    assertEquals(IndexMetaData.IndexState.ERROR, metaData.getState());

    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }

  public void testIndexPickupAfterMasterRestart() throws Exception {
    MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();

    final NodeStartThread nodeStartThread = startNode(new LuceneServer());
    masterStartThread.join();
    nodeStartThread.join();
    waitForPath(zkClientMaster, _conf.getZKMasterPath());
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 1);

    // add index
    final File indexFile = TestResources.INDEX1;
    final int shardCount = indexFile.list(FileUtil.VISIBLE_FILES_FILTER).length;

    final Katta katta = new Katta(_conf);
    final String index = "indexA";
    katta.addIndex(index, "file://" + indexFile.getAbsolutePath(), 2);
    assertEquals(shardCount, zkClientMaster.countChildren(_conf.getZKIndexPath(index)));

    // restartmaster
    masterStartThread.shutdown();
    masterStartThread = startMaster();
    masterStartThread.join();
    assertEquals(1, masterStartThread.getMaster().getIndexes().size());

    nodeStartThread.shutdown();
    masterStartThread.shutdown();
  }

  public void testReplicateUnderreplicatedIndexesAfterNodeAdding() throws Exception {
    final MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();
    final ZkClient zkClient = ZkKattaUtil.startZkClient(_conf, 5000);
    final Master master = masterStartThread.getMaster();

    // start one node
    final NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    masterStartThread.join();
    waitOnNodes(masterStartThread, 1);

    // add index with replication level of 2
    final File indexFile = TestResources.INDEX1;
    final String index = "indexA";
    final DeployClient deployClient = new DeployClient(zkClient, _conf);
    final IIndexDeployFuture deployFuture = deployClient.addIndex(index, "file://" + indexFile.getAbsolutePath(), 2);
    deployFuture.joinDeployment();
    assertEquals(1, deployClient.getIndexes(IndexState.DEPLOYED).size());
    final List<String> shards = zkClient.getChildren(_conf.getZKIndexPath(index));
    for (final String shard : shards) {
      assertEquals(1, zkClient.countChildren(_conf.getZKShardToNodePath(shard)));
    }

    // start node2
    zkClientMaster.getEventLock().lock();
    NodeStartThread nodeStartThread2;
    try {
      nodeStartThread2 = startNode(new LuceneServer(), SECOND_SHARD_FOLDER);
      zkClientMaster.getEventLock().getDataChangedCondition().await();
    } finally {
      zkClientMaster.getEventLock().unlock();
    }
    assertEquals(2, master.readNodes().size());

    // replication should now take place
    for (final String shard : shards) {
      waitForChilds(zkClient, _conf.getZKShardToNodePath(shard), 2);
    }

    deployClient.disconnect();
    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
  }
}
