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

import java.util.concurrent.TimeUnit;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.client.Client;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class FailTest extends AbstractKattaTest {

  public void testMasterFail() throws Exception {
    final ZKClient masterClient = new ZKClient(_conf);
    final ZKClient secMasterClient = new ZKClient(_conf);
    final NodeStartThread nodeThread = startNode();

    final Master master = new Master(masterClient);
    master.start();

    // start secondary master..
    final Master secMaster = new Master(secMasterClient);
    secMaster.start();

    nodeThread.join();
    waitForPath(masterClient, ZkPathes.MASTER);
    waitForChilds(masterClient, ZkPathes.NODES, 1);

    // kill master
    secMasterClient.getEventLock().lock();
    try {
      masterClient.close();
      secMasterClient.getEventLock().getDataChangedCondition().await(30, TimeUnit.SECONDS);
    } finally {
      secMasterClient.getEventLock().unlock();
    }
    // just make sure we can read the file
    waitForPath(secMasterClient, ZkPathes.MASTER);
    assertTrue(secMaster.isMaster());

    nodeThread.shutdown();
    secMasterClient.close();

    try {
      master.shutdown();
    } catch (final Exception e) {
      // zkClient is already down, we just want to interrupt the
      // manage-shard-thread
    }
  }

  public void testNodeFailure() throws Exception {
    final MasterStartThread masterThread = startMaster();
    final ZKClient zkClientMaster = masterThread.getZkClient();

    // create 3 nodes
    final NodeConfiguration sconf1 = new NodeConfiguration();
    final String defaulFolder = sconf1.getShardFolder().getAbsolutePath();
    sconf1.setShardFolder(defaulFolder + "/" + 1);
    final DummyNode node1 = new DummyNode(_conf, sconf1);

    final NodeConfiguration sconf2 = new NodeConfiguration();
    final String defaulFolder2 = sconf2.getShardFolder().getAbsolutePath();
    sconf2.setShardFolder(defaulFolder2 + "/" + 2);
    final DummyNode node2 = new DummyNode(_conf, sconf2);

    final NodeConfiguration sconf3 = new NodeConfiguration();
    final String defaulFolder3 = sconf3.getShardFolder().getAbsolutePath();
    sconf3.setShardFolder(defaulFolder3 + "/" + 3);
    final DummyNode node3 = new DummyNode(_conf, sconf3);
    waitForChilds(zkClientMaster, ZkPathes.NODES, 3);
    masterThread.join();
    waitForPath(zkClientMaster, ZkPathes.MASTER);

    // deploy index
    final IDeployClient deployClient = new DeployClient(_conf);
    final String indexName = "index";
    deployClient.addIndex(indexName, TestResources.UNZIPPED_INDEX.getAbsolutePath(), StandardAnalyzer.class.getName(),
        3).joinDeployment();
    final Client client = new Client();
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));
    assertEquals(1, node1.countShards());
    assertEquals(1, node2.countShards());
    assertEquals(1, node3.countShards());
    node1.close();
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));
    node2.close();
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));

    // add count Shards to Node Object... and check why no reasignment
    // happens....

    // kill 2 nodes

    // we should be still be able to search

    // bring back 2 nodes

    // things should be good distributed again.
    client.close();
    deployClient.disconnect();
    node3.close();
    masterThread.shutdown();
  }

  private class DummyNode {

    private final ZKClient _client;
    private final Node _node;

    public DummyNode(final ZkConfiguration conf, final NodeConfiguration nodeConfiguration) throws KattaException {
      _client = new ZKClient(conf);
      _node = new Node(_client, nodeConfiguration);
      _node.start();
    }

    public int countShards() {
      return _node.getDeployedShards().size();
    }

    void close() {
      _client.close();
      _node.shutdown();
    }
  }

}
