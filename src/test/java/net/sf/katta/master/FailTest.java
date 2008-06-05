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

import junit.framework.TestCase;
import net.sf.katta.Katta;
import net.sf.katta.ZkServer;
import net.sf.katta.client.Client;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.SlaveConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class FailTest extends TestCase {

  public void testMasterFail() throws Exception {
    final ZkConfiguration zkConf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(zkConf);
    final ZKClient client = new ZKClient(zkConf);
    client.waitForZooKeeper(100000);
    final ZKClient slaveClient = new ZKClient(zkConf);
    slaveClient.waitForZooKeeper(100000);
    final ZKClient masterClient = new ZKClient(zkConf);
    masterClient.waitForZooKeeper(100000);
    final ZKClient secMasterClient = new ZKClient(zkConf);
    secMasterClient.waitForZooKeeper(100000);

    cleanNameSpace(client);

    final Master master = new Master(masterClient);
    master.start();
    waitFor(client, IPaths.MASTER);

    final Node slave = new Node(slaveClient);
    slave.start();
    waitFor(client, IPaths.SLAVES, 1);

    // start secondary master..
    final Master secMaster = new Master(secMasterClient);
    secMaster.start();
    // kill master
    masterClient.close();
    int count = 0;
    while (!secMaster.isMaster() && count++ < 100) {
      Thread.sleep(1000);
    }

    // just make sure we can read the file
    waitFor(client, IPaths.MASTER);

    assertTrue(secMaster.isMaster());
    zkServer.shutdown();
    client.close();
    slaveClient.close();
    secMasterClient.close();
  }

  private void cleanNameSpace(final ZKClient client) throws KattaException {
    final String kattaPath = "/katta";
    if (client.exists(kattaPath)) {
      client.deleteRecursiv(kattaPath);
    }
  }

  public void testSlaveFailure() throws Exception {

    final ZkConfiguration zkConf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(zkConf);
    final ZKClient zkClient = new ZKClient(zkConf);
    zkClient.waitForZooKeeper(100000);
    // TODO we did run in issues in case the index is already deployed,so we
    // should check this..
    cleanNameSpace(zkClient);
    final ZKClient masterClient = new ZKClient(zkConf);

    final Master master = new Master(masterClient);
    master.start();
    waitFor(zkClient, IPaths.MASTER);

    // create 3 slaves
    final SlaveConfiguration sconf = new SlaveConfiguration();
    final String defaulFolder = sconf.getShardFolder();
    sconf.setShardFolder(defaulFolder + "/" + 1);
    final DummySlave s1 = new DummySlave(zkConf, sconf);
    sconf.setShardFolder(defaulFolder + "/" + 2);
    final DummySlave s2 = new DummySlave(zkConf, sconf);
    sconf.setShardFolder(defaulFolder + "/" + 2);
    final DummySlave s3 = new DummySlave(zkConf, sconf);
    waitFor(zkClient, IPaths.SLAVES, 3);
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

    // add count Shards to Slave Object... and check why no reasignment
    // happens....

    // kill 2 slaves

    // we should be still be able to search

    // bring back 2 slaves

    // things should be good distributed again.
    katta.close();
    masterClient.close();
    zkServer.shutdown();
    client.close();
    s1.close();
    s2.close();
    s3.close();

  }

  private void waitFor(final ZKClient client, final String path, final int size) throws InterruptedException,
  KattaException {
    int count = 0;
    while (client.getChildren(path).size() != size && count++ < 100) {
      Thread.sleep(1000);
    }

  }

  private void waitFor(final ZKClient client, final String path) throws KattaException, InterruptedException {
    int count = 0;
    while (!client.exists(path) && count++ < 100) {
      Thread.sleep(1000);
    }
  }

  private class DummySlave {

    private final ZKClient _client;
    private final Node _slave;

    public DummySlave(final ZkConfiguration conf, final SlaveConfiguration slaveConfiguration) throws KattaException {
      _client = new ZKClient(conf);
      _slave = new Node(_client, slaveConfiguration);
      _slave.start();
    }

    public int countShards() {
      return _slave.getDeployShards().size();
    }

    void close() {
      _client.close();
      _slave.shutdown();
    }
  }

}
