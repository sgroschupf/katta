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
package net.sf.katta.client;

import java.util.List;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Query;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class ClientFailoverTest extends AbstractKattaTest {

  MasterStartThread masterThread;
  NodeStartThread nodeThread1;
  NodeStartThread nodeThread2;
  String index = "index1";

  static int nodePort1 = 20000;
  static int nodePort2 = 20001;

  @Override
  protected void onSetUp2() throws Exception {
    masterThread = startMaster();
    nodeThread1 = startNode(nodePort1);
    nodeThread2 = startNode("/tmp/kattaShards2", nodePort2);

    masterThread.join();
    nodeThread1.join();
    nodeThread2.join();

    // distribute index over 2 nodes
    Katta katta = new Katta();
    katta.addIndex(index, "src/test/testIndexA", StandardAnalyzer.class.getName(), 2);
    katta.close();
  }

  @Override
  protected void onTearDown() throws Exception {
    masterThread.shutdown();
    // jz: since hadoop18 the ipc acts a little fragile when starting and
    // stopping ipc-servers rapidly on the same port so we increment port
    // numbers from test to test
    nodePort1 = nodePort1 + 4;
    nodePort2 = nodePort2 + 4;
  }

  public void testSearch_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    Client searchClient = new Client();

    // shutdown proxy of node1
    nodeThread1.getNode().getRpcServer().stop();

    final Query query = new Query("content:the");
    System.out.println("=========================");
    assertSearchResults(10, searchClient.search(query, new String[] { index }, 10));
    assertSearchResults(10, searchClient.search(query, new String[] { index }, 10));
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");

    nodeThread1.shutdown();
    nodeThread2.shutdown();
    searchClient.close();
  }

  public void testCount_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    Client searchClient = new Client();

    // shutdown proxy of node1
    nodeThread1.getNode().getRpcServer().stop();

    final Query query = new Query("content:the");
    System.out.println("=========================");
    assertEquals(937, searchClient.count(query, new String[] { index }));
    assertEquals(937, searchClient.count(query, new String[] { index }));
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");

    nodeThread1.shutdown();
    nodeThread2.shutdown();
    searchClient.close();
  }

  public void testGetDetails_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    Client searchClient = new Client();
    final Query query = new Query("content:the");
    Hits hits = searchClient.search(query, new String[] { index }, 10);

    // shutdown proxy of node1
    System.out.println("=========================");
    if (nodeThread1.getNode().getName().equals(hits.getHits().get(0).getNode())) {
      nodeThread1.getNode().getRpcServer().stop();
    } else {
      nodeThread2.getNode().getRpcServer().stop();
    }
    assertFalse(searchClient.getDetails(hits.getHits().get(0)).isEmpty());
    assertFalse(searchClient.getDetails(hits.getHits().get(0)).isEmpty());
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");

    nodeThread1.shutdown();
    nodeThread2.shutdown();
    searchClient.close();
  }

  public void testAllNodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    Client searchClient = new Client();
    final Query query = new Query("content:the");
    nodeThread1.getNode().getRpcServer().stop();
    nodeThread2.getNode().getRpcServer().stop();

    System.out.println("=========================");
    try {
      searchClient.search(query, new String[] { index }, 10);
      fail("should throw exception");
    } catch (ShardAccessException e) {
      // expected
    }
    System.out.println("=========================");

    nodeThread1.shutdown();
    nodeThread2.shutdown();
    searchClient.close();
  }

  private void assertSearchResults(int expectedResults, Hits hits) {
    assertNotNull(hits);
    assertEquals(expectedResults, hits.getHits().size());
  }

  public void testNodeNotReachable() throws Exception {
    // shutdown 2nd node
    nodeThread2.shutdown();
    waitOnNodes(masterThread, 1);

    // simulate 2nd node alive and serving shard
    String node2Name = nodeThread2.getNode().getName();
    ZKClient zkClient = masterThread.getZkClient();
    List<String> shards = zkClient.getChildren(ZkPathes.getIndexPath(index));
    zkClient.create(ZkPathes.getNodePath(node2Name));
    for (String shard : shards) {
      zkClient.create(ZkPathes.getShard2NodePath(shard, node2Name));
    }
    waitOnNodes(masterThread, 2);

    // start search client
    Client searchClient = new Client();
    final Query query = new Query("content:the");
    assertSearchResults(10, searchClient.search(query, new String[] { index }, 10));
    assertSearchResults(10, searchClient.search(query, new String[] { index }, 10));

    // flip node1/node2 alive status
    nodeThread2 = startNode("/tmp/kattaShards2", nodePort2);
    nodeThread2.join();
    nodeThread1.shutdown();
    waitOnNodes(masterThread, 1);

    // search again
    assertSearchResults(10, searchClient.search(query, new String[] { index }, 10));
    searchClient.close();
  }

}
