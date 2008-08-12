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

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.TimingTestUtil;
import net.sf.katta.client.Client;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class FailTest extends AbstractKattaTest {

  public void testMasterFail() throws Exception {
    createZkServer();
    final ZKClient client = new ZKClient(conf);
    client.start(100000);
    final ZKClient nodeClient = new ZKClient(conf);
    final ZKClient masterClient = new ZKClient(conf);
    final ZKClient secMasterClient = new ZKClient(conf);

    final Node node = new Node(nodeClient);
    Thread clientThread = new Thread(new Runnable() {
      // the masters start() methods are blocking unitl at least one node is
      // connected, so we start the node in a seperate thread
      public void run() {
        try {
          node.start();
        } catch (KattaException e) {
          e.printStackTrace();
        }
      }
    });
    clientThread.start();

    final Master master = new Master(masterClient);
    master.start();

    // start secondary master..
    final Master secMaster = new Master(secMasterClient);
    secMaster.start();

    clientThread.join();
    TimingTestUtil.waitFor(client, IPaths.MASTER);
    TimingTestUtil.waitFor(client, IPaths.NODES, 1);

    // kill master
    masterClient.close();
    int count = 0;
    while (!secMaster.isMaster() && count++ < 100) {
      Thread.sleep(1000);
    }

    // just make sure we can read the file
    TimingTestUtil.waitFor(client, IPaths.MASTER);

    assertTrue(secMaster.isMaster());
    client.close();
    nodeClient.close();
    secMasterClient.close();
  }

  public void testNodeFailure() throws Exception {
    createZkServer();
    final ZKClient zkClient = new ZKClient(conf);
    zkClient.start(100000);
    // TODO we did run in issues in case the index is already deployed,so we
    // should check this..
    final ZKClient masterClient = new ZKClient(conf);

    final Master master = new Master(masterClient);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();
    TimingTestUtil.waitFor(zkClient, IPaths.MASTER);

    // create 3 nodes
    final NodeConfiguration sconf1 = new NodeConfiguration();
    final String defaulFolder = sconf1.getShardFolder();
    sconf1.setShardFolder(defaulFolder + "/" + 1);
    final DummyNode s1 = new DummyNode(conf, sconf1);

    final NodeConfiguration sconf2 = new NodeConfiguration();
    final String defaulFolder2 = sconf2.getShardFolder();
    sconf2.setShardFolder(defaulFolder2 + "/" + 2);
    final DummyNode s2 = new DummyNode(conf, sconf2);

    final NodeConfiguration sconf3 = new NodeConfiguration();
    final String defaulFolder3 = sconf3.getShardFolder();
    sconf3.setShardFolder(defaulFolder3 + "/" + 3);
    final DummyNode s3 = new DummyNode(conf, sconf3);
    TimingTestUtil.waitFor(zkClient, IPaths.NODES, 3);
    masterThread.join();
    // deploy index

    final Katta katta = new Katta();
    final String indexName = "index";
    katta.addIndex(indexName, "src/test/testIndexC/", StandardAnalyzer.class.getName(), 3);
    final Client client = new Client();
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));
    zkClient.showFolders();
    assertEquals(1, s1.countShards());
    assertEquals(1, s2.countShards());
    assertEquals(1, s3.countShards());
    s1.close();
    Thread.sleep(10000);
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));
    s2.close();
    Thread.sleep(2000);
    assertEquals(2, client.count(new Query("foo:bar"), new String[] { indexName }));

    // add count Shards to Node Object... and check why no reasignment
    // happens....

    // kill 2 nodes

    // we should be still be able to search

    // bring back 2 nodes

    // things should be good distributed again.
    katta.close();
    masterClient.close();
    client.close();
    s1.close();
    s2.close();
    s3.close();

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
      return _node.getDeployShards().size();
    }

    void close() {
      _client.close();
      _node.shutdown();
    }
  }

}
