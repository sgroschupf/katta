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
import net.sf.katta.ZkServer;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.IPaths;
import net.sf.katta.master.Master;
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

  private ZkServer _server;
  private ZKClient _zkclient;
  private Slave _server1;
  private Slave _server2;
  private Master _master;

  @Override
  protected void setUp() throws Exception {
    // public void testname() throws Exception {

    final ZkConfiguration conf = new ZkConfiguration();
    _server = new ZkServer(conf);
    _zkclient = new ZKClient(conf);
    _zkclient.waitForZooKeeper(600000);
    if (_zkclient.exists(IPaths.ROOT_PATH)) {
      _zkclient.deleteRecursiv(IPaths.ROOT_PATH);
    }
    _master = new Master(_zkclient);
    _master.start();

    _server1 = SlaveServerTest.startSlaveServer(_zkclient);
    _server2 = SlaveServerTest.startSlaveServer(_zkclient);
    while (_zkclient.getChildren(IPaths.SLAVES).size() != 2) {
      Thread.sleep(500);
    }
    // _zkclient.showFolders(System.out);
    final IndexMetaData index = new IndexMetaData("src/test/testIndexA/", StandardAnalyzer.class.getName(), false);
    _zkclient.create(IPaths.INDEXES + "/index", index);
    // _zkclient.showFolders(System.out);
    // wait for index deployment.
    final IndexMetaData data0 = new IndexMetaData("", "", false);
    while (!data0.isDeployed()) {
      Thread.sleep(500);
      _zkclient.readData(IPaths.INDEXES + "/index", data0);
    }
    final IndexMetaData indexA = new IndexMetaData("src/test/testIndexA", StandardAnalyzer.class.getName(), false);
    _zkclient.create(IPaths.INDEXES + "/index1", indexA);

    final IndexMetaData indexB = new IndexMetaData("src/test/testIndexB", StandardAnalyzer.class.getName(), false);
    _zkclient.create(IPaths.INDEXES + "/index2", indexB);
    // wait for slaves

    // wait for index deployment.
    final IndexMetaData data1 = new IndexMetaData("", "", false);
    final IndexMetaData data2 = new IndexMetaData("", "", false);
    while (!data1.isDeployed() || !data2.isDeployed()) {
      Thread.sleep(500);
      _zkclient.readData(IPaths.INDEXES + "/index1", data1);
      _zkclient.readData(IPaths.INDEXES + "/index2", data2);
    }

  }

  //
  // @Override
  @Override
  protected void tearDown() throws Exception {
    _server1.shutdown();
    _server2.shutdown();
    _server.shutdown();
    _zkclient.close();
    RPC.stopClient();
  }

  //
  public void testCount() throws InterruptedException, IOException, ParseException, KattaException {
    System.out.println("ClientTest.testCount()");
    final IClient client = new Client();
    final Query query = new Query("content: the");
    final int count = client.count(query, new String[] { "index" });
    assertEquals(937, count);
    client.close();
  }

  public void testGetDetails() throws InterruptedException, IOException, ParseException, KattaException {
    System.out.println("ClientTest.testGetDetails()");
    final IClient client = new Client();
    final Query query = new Query("content:the");
    final Hits hits = client.search(query, new String[] { "index" }, 10);
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
  }

  public void testSearch() throws KattaException, InterruptedException, IOException {
    System.out.println("ClientTest.testSearch()");
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
  }

  public void testSearchLimit() throws InterruptedException, IOException, ParseException, KattaException {
    System.out.println("ClientTest.testSearchLimit()");
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
  }

  //
  public void testSearchSimiliarity() throws InterruptedException, IOException, ParseException, KattaException {
    System.out.println("ClientTest.testSearchSimiliarity()");

    final IClient client = new Client();
    final Query query = new Query("foo: bar");
    final Hits hits = client.search(query, new String[] { "index1" });
    assertNotNull(hits);
    System.out.println(hits);
    assertEquals(4, hits.getHits().size());
    for (final Hit hit : hits.getHits()) {
      Logger.info(hit.getSlave() + " -- " + hit.getScore() + " -- " + hit.getDocId());
    }

  }
}
