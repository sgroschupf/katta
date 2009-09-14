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
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Query;

import org.I0Itec.zkclient.ZkClient;

public class LuceneClientFailoverTest extends AbstractKattaTest {

  private MasterStartThread _masterThread;
  private NodeStartThread _nodeThread1;
  private NodeStartThread _nodeThread2;
  private String index = "index1";

  static int nodePort1 = 20000;
  static int nodePort2 = 20001;
  private LuceneClient _searchClient;

  @Override
  protected void onSetUp2() throws Exception {
    _masterThread = startMaster();
    _nodeThread1 = startNode(new LuceneServer(), nodePort1);
    _nodeThread2 = startNode(new LuceneServer(), nodePort2, "./build/data/LuceneClientFailoverTest/kattaShards2");

    _masterThread.join();
    _nodeThread1.join();
    _nodeThread2.join();

    // distribute index over 2 nodes
    Katta katta = new Katta(_conf);
    katta.addIndex(index, "src/test/testIndexA", 2);
    katta.close();
  }

  @Override
  protected void onTearDown() throws Exception {
    if (_searchClient != null) {
      _searchClient.close();
    }
    _nodeThread1.shutdown();
    _nodeThread2.shutdown();
    _masterThread.shutdown();
    // jz: since hadoop18 the ipc acts a little fragile when starting and
    // stopping ipc-servers rapidly on the same port so we increment port
    // numbers from test to test
    nodePort1 = nodePort1 + 4;
    nodePort2 = nodePort2 + 4;
  }

  public void testSearch_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    _searchClient = new LuceneClient(_conf);

    // shutdown proxy of node1
    _nodeThread1.getNode().getRpcServer().stop();

    final Query query = new Query("content:the");
    System.out.println("=========================");
    assertSearchResults(10, _searchClient.search(query, new String[] { index }, 10));
    assertSearchResults(10, _searchClient.search(query, new String[] { index }, 10));
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");
  }

  public void testCount_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    _searchClient = new LuceneClient(_conf);

    // shutdown proxy of node1
    _nodeThread1.getNode().getRpcServer().stop();

    final Query query = new Query("content:the");
    System.out.println("=========================");
    assertEquals(937, _searchClient.count(query, new String[] { index }));
    assertEquals(937, _searchClient.count(query, new String[] { index }));
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");
  }

  public void testGetDetails_NodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    _searchClient = new LuceneClient(_conf);
    final Query query = new Query("content:the");
    Hits hits = _searchClient.search(query, new String[] { index }, 10);

    // shutdown proxy of node1
    System.out.println("=========================");
    if (_nodeThread1.getNode().getName().equals(hits.getHits().get(0).getNode())) {
      _nodeThread1.getNode().getRpcServer().stop();
    } else {
      _nodeThread2.getNode().getRpcServer().stop();
    }
    assertFalse(_searchClient.getDetails(hits.getHits().get(0)).isEmpty());
    assertFalse(_searchClient.getDetails(hits.getHits().get(0)).isEmpty());
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");
  }

  public void testAllNodeProxyDownAfterClientInitialization() throws Exception {
    // start search client
    _searchClient = new LuceneClient(_conf);
    final Query query = new Query("content:the");
    _nodeThread1.getNode().getRpcServer().stop();
    _nodeThread2.getNode().getRpcServer().stop();

    System.out.println("=========================");
    try {
      _searchClient.search(query, new String[] { index }, 10);
      fail("should throw exception");
    } catch (ShardAccessException e) {
      // expected
    }
    System.out.println("=========================");
  }

  private void assertSearchResults(int expectedResults, Hits hits) {
    assertNotNull(hits);
    assertEquals(expectedResults, hits.getHits().size());
  }

  public void testNodeNotReachable() throws Exception {
    // shutdown 2nd node
    _nodeThread2.shutdown();
    waitOnNodes(_masterThread, 1);

    // simulate 2nd node alive and serving shard
    String node2Name = _nodeThread2.getNode().getName();
    ZkClient zkClient = _masterThread.getZkClient();
    List<String> shards = zkClient.getChildren(_conf.getZKIndexPath(index));
    zkClient.createPersistent(_conf.getZKNodePath(node2Name));
    for (String shard : shards) {
      zkClient.createPersistent(_conf.getZKShardToNodePath(shard, node2Name));
    }
    waitOnNodes(_masterThread, 2);

    _searchClient = new LuceneClient(_conf);
    final Query query = new Query("content:the");
    assertSearchResults(10, _searchClient.search(query, new String[] { index }, 10));
    assertSearchResults(10, _searchClient.search(query, new String[] { index }, 10));

    // flip node1/node2 alive status
    _nodeThread2 = startNode(new LuceneServer(), nodePort2, "./build/data/LuceneClientFailoverTest/kattaShards2");
    _nodeThread2.join();
    _nodeThread1.shutdown();
    waitOnNodes(_masterThread, 1);

    // search again
    assertSearchResults(10, _searchClient.search(query, new String[] { index }, 10));
    _searchClient.close();
  }
}
