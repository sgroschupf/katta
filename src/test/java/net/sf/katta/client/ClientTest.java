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
package net.sf.katta.client;

import java.io.IOException;
import java.util.Set;

import junit.framework.TestCase;
import net.sf.katta.Server;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.IPaths;
import net.sf.katta.slave.Hit;
import net.sf.katta.slave.Hits;
import net.sf.katta.slave.Query;
import net.sf.katta.slave.Slave;
import net.sf.katta.slave.SlaveServerTest;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;

public class ClientTest extends TestCase {

  @Override
  protected void setUp() throws Exception {
    Thread.sleep(1000);
  }

  public void testSearch() throws KattaException, InterruptedException, IOException {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server master = new Server(conf);
    final ZKClient zkclient = new ZKClient(conf);
    zkclient.waitForZooKeeper(5000);
    if (zkclient.exists(IPaths.ROOT_PATH)) {
      zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    master.startMasterOrSlave(zkclient, true);

    final Slave server1 = SlaveServerTest.startSlaveServer("/katta.zk.slave1.properties");
    final Slave server2 = SlaveServerTest.startSlaveServer("/katta.zk.slave2.properties");

    while (zkclient.getChildren(IPaths.SLAVES).size() != 2) {
      Thread.sleep(500);
    }

    final IndexMetaData indexA = new IndexMetaData("src/test/testIndexA", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index1", indexA);

    final IndexMetaData indexB = new IndexMetaData("src/test/testIndexB", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index2", indexB);
    // wait for slaves

    // wait for index deployment.
    final IndexMetaData data = new IndexMetaData("", "", false);
    final IndexMetaData data2 = new IndexMetaData("", "", false);
    while (!data.isDeployed() || !data2.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index1", data);
      zkclient.readData(IPaths.INDEXES + "/index2", data2);
    }

    final IClient client = new Client();

    final Query query = new Query("foo: bar");
    final Hits hits = client.search(query, new String[] { "index2", "index1" });
    assertNotNull(hits);
    assertEquals(1f, client.getQueryPerMinute());
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    assertEquals(8, hits.size());
    assertEquals(8, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    zkclient.close();
    server1.shutdown();
    server2.shutdown();
    RPC.stopClient();
    master.shutdown();
  }

  public void testCount() throws InterruptedException, IOException, ParseException, KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZKClient zkclient = new ZKClient(conf);
    final Server master = new Server(conf);
    zkclient.waitForZooKeeper(5000);

    if (zkclient.exists(IPaths.ROOT_PATH)) {
      zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    master.startMasterOrSlave(zkclient, true);

    final Slave server = SlaveServerTest.startSlaveServer("/katta.zk.slave1.properties");
    while (zkclient.getChildren(IPaths.SLAVES).size() != 1) {
      Thread.sleep(500);
    }

    final IndexMetaData index = new IndexMetaData("src/test/testIndexA/", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index", index);

    // wait for index deployment.
    final IndexMetaData data = new IndexMetaData("", "", false);
    while (!data.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index", data);
    }
    final IClient client = new Client();
    final Query query = new Query("content: the");
    final int count = client.count(query, new String[] { "index" });
    assertEquals(937, count);
    zkclient.close();
    server.shutdown();
    RPC.stopClient();
    master.shutdown();
  }

  public void testSearchSimiliarity() throws InterruptedException, IOException, ParseException, KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZKClient zkclient = new ZKClient(conf);
    final Server master = new Server(conf);
    zkclient.waitForZooKeeper(5000);
    if (zkclient.exists(IPaths.ROOT_PATH)) {
      zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    master.startMasterOrSlave(zkclient, true);

    final Slave server = SlaveServerTest.startSlaveServer("/katta.zk.slave1.properties");
    while (zkclient.getChildren(IPaths.SLAVES).size() != 1) {
      Thread.sleep(500);
    }

    final IndexMetaData index1 = new IndexMetaData("src/test/testIndexC/", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index1", index1);
    // wait for index deployment.
    final IndexMetaData data = new IndexMetaData("", "", false);
    while (!data.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index1", data);
    }
    final IClient client = new Client();
    final Query query = new Query("foo: bar");
    final Hits hits = client.search(query, new String[] { "index1" });
    assertNotNull(hits);
    assertEquals(2, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    zkclient.close();
    server.shutdown();
    RPC.stopClient();
    Thread.sleep(3000);
    master.shutdown();
  }

  public void testGetDetails() throws InterruptedException, IOException, ParseException, KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZKClient zkclient = new ZKClient(conf);
    final Server master = new Server(conf);
    zkclient.waitForZooKeeper(5000);
    if (zkclient.exists(IPaths.ROOT_PATH)) {
      zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    master.startMasterOrSlave(zkclient, true);

    final Slave server = SlaveServerTest.startSlaveServer("/katta.zk.slave1.properties");
    while (zkclient.getChildren(IPaths.SLAVES).size() != 1) {
      Thread.sleep(500);
    }

    final IndexMetaData indexA = new IndexMetaData("src/test/testIndexA/", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index1", indexA);
    // wait for index deployment.
    final IndexMetaData data = new IndexMetaData("", "", false);
    while (!data.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index1", data);
    }

    final IClient client = new Client();
    final Query query = new Query("content:the");
    final Hits hits = client.search(query, new String[] { "index1" }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      final MapWritable details = client.getDetails(hit);
      final Set<Writable> keySet = details.keySet();
      assertFalse(keySet.isEmpty());
      for (final Writable writable : keySet) {
        System.out.println(writable);
      }
      final Writable writable = details.get(new Text("path"));
      assertNotNull(writable);
    }
    zkclient.close();
    server.shutdown();
    RPC.stopClient();
    master.shutdown();
  }

  public void testSearchLimit() throws InterruptedException, IOException, ParseException, KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server master = new Server(conf);
    final ZKClient zkclient = new ZKClient(conf);
    zkclient.waitForZooKeeper(5000);
    if (zkclient.exists(IPaths.ROOT_PATH)) {
      zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    master.startMasterOrSlave(zkclient, true);

    final Slave server1 = SlaveServerTest.startSlaveServer("/katta.zk.slave1.properties");
    final Slave server2 = SlaveServerTest.startSlaveServer("/katta.zk.slave2.properties");

    while (zkclient.getChildren(IPaths.SLAVES).size() != 2) {
      Thread.sleep(500);
    }

    final IndexMetaData indexA = new IndexMetaData("src/test/testIndexA", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index1", indexA);

    final IndexMetaData indexB = new IndexMetaData("src/test/testIndexB", StandardAnalyzer.class.getName(), false);
    zkclient.create(IPaths.INDEXES + "/index2", indexB);

    // wait for index deployment.
    IndexMetaData data = new IndexMetaData("", "", false);
    while (!data.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index1", data);
    }
    data = new IndexMetaData("", "", false);
    while (!data.isDeployed()) {
      Thread.sleep(500);
      zkclient.readData(IPaths.INDEXES + "/index2", data);
    }

    final IClient client = new Client();
    final Query query = new Query("foo: bar");
    final Hits hits = client.search(query, new String[] { "index2", "index1" }, 1);
    assertNotNull(hits);
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getShard() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    assertEquals(8, hits.size());
    assertEquals(1, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }
    zkclient.close();
    server1.shutdown();
    server2.shutdown();
    RPC.stopClient();
    master.shutdown();
  }
}
