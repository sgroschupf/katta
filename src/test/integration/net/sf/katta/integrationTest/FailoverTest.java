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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.client.IndexState;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.TestUtil;

import org.I0Itec.zkclient.ZkClient;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Test;

public class FailoverTest extends AbstractIntegrationTest {

  public FailoverTest() {
    super(3, true);
  }

  @Test(timeout = 20000)
  public void testMasterFail() throws Exception {
    // start secondary master..
    Master secondaryMaster = _miniCluster.startSecondaryMaster();
    assertTrue(_miniCluster.getMaster().isMaster());
    assertFalse(secondaryMaster.isMaster());

    // kill master
    _miniCluster.getMaster().shutdown();

    // just make sure we can read the file
    TestUtil.waitUntilBecomeMaster(secondaryMaster);
    shutdownCluster();
  }

  @Test(timeout = 50000)
  public void testNodeFailure() throws Exception {
    deployTestIndices(1, getNodeCount());
    final LuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    Query query = new QueryParser("", new KeywordAnalyzer()).parse("foo:bar");
    assertEquals(4, client.count(query, new String[] { INDEX_NAME }));

    // kill 1st of 3 nodes
    _miniCluster.shutdownNode(0);
    assertEquals(4, client.count(query, new String[] { INDEX_NAME }));

    // kill 2nd of 3 nodes
    _miniCluster.shutdownNode(0);
    assertEquals(4, client.count(query, new String[] { INDEX_NAME }));

    // add a 4th node
    Node node4 = _miniCluster.startAdditionalNode();
    TestUtil.waitUntilNodeServesShards(_protocol, node4.getName(), SHARD_COUNT);
    assertEquals(4, client.count(query, new String[] { INDEX_NAME }));

    // kill 3rd node
    Thread.sleep(5000);
    _miniCluster.shutdownNode(0);
    assertEquals(4, client.count(query, new String[] { INDEX_NAME }));
  }

  @Test(timeout = 50000)
  public void testZkMasterReconnectDuringDeployment() throws Exception {
    deployTestIndices(1, getNodeCount());
    _miniCluster.getMaster().shutdown();

    ZkClient zkClient = new ZkClient(_miniCluster.getZkConfiguration().getZKServers());
    InteractionProtocol protocol = new InteractionProtocol(zkClient, _miniCluster.getZkConfiguration());
    Master master = new Master(protocol, false);
    master.start();
    TestUtil.waitUntilBecomeMaster(master);

    final IDeployClient deployClient = new DeployClient(_protocol);
    WatchedEvent event = new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.Expired
            .getIntValue(), null));
    for (int i = 0; i < 25; i++) {
      final String indexName = "index" + i;
      IIndexDeployFuture deployFuture = deployClient.addIndex(indexName, INDEX_FILE.getAbsolutePath(), 1);
      zkClient.getEventLock().lock();
      zkClient.process(event);
      zkClient.getEventLock().unlock();
      IndexState indexState = deployFuture.joinDeployment();
      assertEquals(IndexState.DEPLOYED, indexState);
      if (indexState == IndexState.ERROR) {
        IndexDeployError deployError = protocol.getIndexMD(indexName).getDeployError();
        Set<Shard> shards = protocol.getIndexMD(indexName).getShards();
        for (Shard shard : shards) {
          List<Exception> shardErrors = deployError.getShardErrors(shard.getName());
          for (Exception exception : shardErrors) {
            exception.printStackTrace();
          }
        }
        deployError.getException().printStackTrace();
      }
    }
  }

}
