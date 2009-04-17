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
package net.sf.katta.node;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.mockito.Mockito;

public class NodeTest extends AbstractKattaTest {

  public void testShardStatusSuccess() throws Exception {
    MasterStartThread masterThread = startMaster();
    NodeStartThread nodeThread = startNode();
    masterThread.join();
    nodeThread.join();
    waitForChilds(masterThread.getZkClient(), ZkPathes.NODES, 1);

    // deploy index
    Katta katta = new Katta();
    katta.addIndex("index", TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1);

    // test
    final String indexPath = ZkPathes.INDEXES + "/index";
    IndexMetaData indexMetaData = new IndexMetaData();
    masterThread.getZkClient().readData(indexPath, indexMetaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, indexMetaData.getState());

    // close all
    katta.close();
    nodeThread.shutdown();
    masterThread.shutdown();
  }

  public void testShardStatusNoSuccessNoIndexGiven() throws Exception {
    MasterStartThread masterThread = startMaster();
    NodeStartThread nodeThread = startNode();
    masterThread.join();
    nodeThread.join();
    waitForChilds(masterThread.getZkClient(), ZkPathes.NODES, 1);

    // deploy index
    Katta katta = new Katta();
    katta.addIndex("index", "src/test/testIndexNotHere/", StandardAnalyzer.class.getName(), 1);

    // test
    final String indexPath = ZkPathes.INDEXES + "/index";
    IndexMetaData indexMetaData = new IndexMetaData();
    masterThread.getZkClient().readData(indexPath, indexMetaData);
    assertEquals(IndexState.ERROR, indexMetaData.getState());
    assertNotNull(indexMetaData.getErrorMessage());

    // close all
    katta.close();
    nodeThread.shutdown();
    masterThread.shutdown();
  }

  public void testDeployShardAfterRestart() throws Exception {
    MasterStartThread masterThread = startMaster();
    NodeStartThread nodeThread = startNode();
    masterThread.join();
    nodeThread.join();
    waitForChilds(masterThread.getZkClient(), ZkPathes.NODES, 1);

    // deploy index
    Node node = nodeThread.getNode();
    assertEquals(0, node.getDeployedShards().size());
    Katta katta = new Katta();
    String index = "index";
    katta.addIndex(index, TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1);

    // test
    assertTrue(node.getDeployedShards().size() > 0);
    IndexMetaData indexMetaData = new IndexMetaData();
    masterThread.getZkClient().readData(ZkPathes.getIndexPath(index), indexMetaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, indexMetaData.getState());

    nodeThread.shutdown();
    nodeThread = startNode();
    nodeThread.join();
    node = nodeThread.getNode();
    assertTrue(node.getDeployedShards().size() > 0);

    // close all
    katta.close();
    nodeThread.shutdown();
    masterThread.shutdown();
  }

  public void testMultiThreadSearch() throws Exception {

    ZKClient zkClient = Mockito.mock(ZKClient.class);
    Mockito.when(zkClient.getEventLock()).thenReturn(new ZKClient.ZkLock());

    Node node = new Node(zkClient);
    node.start();

    List<AssignedShard> shards = new ArrayList<AssignedShard>();
    shards.add(new AssignedShard("index", "src/test/testIndexA/aIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/bIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/cIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/dIndex"));

    node.deployShards(shards);

    ArrayList<String> shardNames = new ArrayList<String>();
    for (AssignedShard assignedShard : shards) {
      shardNames.add(assignedShard.getShardName());
    }

    QueryParser parser = new QueryParser("field", new KeywordAnalyzer());
    Query query = parser.parse("foo: bar");
    QueryWritable writable = new QueryWritable(query);

    String[] shardArray = shardNames.toArray(new String[shardNames.size()]);
    DocumentFrequenceWritable freqs = node.getDocFreqs(writable, shardArray);

    ExecutorService es = Executors.newFixedThreadPool(100);
    List<Future<HitsMapWritable>> tasks = new ArrayList<Future<HitsMapWritable>>();
    for (int i = 0; i < 10000; i++) {
      QueryClient client = new QueryClient(node, freqs, writable, shardArray);
      Future<HitsMapWritable> future = es.submit(client);
      tasks.add(future);
    }
    HitsMapWritable last = null;
    for (Future<HitsMapWritable> future : tasks) {
      HitsMapWritable hitsMapWritable = future.get();
      if (last == null) {
        last = hitsMapWritable;
      } else {
        Assert.assertEquals(last.getTotalHits(), hitsMapWritable.getTotalHits());
        float lastScore = last.getHits().getHits().get(0).getScore();
        float currentScore = hitsMapWritable.getHits().getHits().get(0).getScore();
        Assert.assertEquals(lastScore, currentScore);
      }
    }
  }

  public void testUndeployShards() throws Exception {
    ZKClient zkClient = Mockito.mock(ZKClient.class);
    Mockito.when(zkClient.getEventLock()).thenReturn(new ZKClient.ZkLock());

    Node node = new Node(zkClient);
    node.start();

    List<AssignedShard> shards = new ArrayList<AssignedShard>();
    shards.add(new AssignedShard("index", "src/test/testIndexA/aIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/bIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/cIndex"));
    shards.add(new AssignedShard("index", "src/test/testIndexA/dIndex"));

    node.deployShards(shards);

    File workingFolder = node._shardsFolder;
    assertEquals(4, workingFolder.list().length);
    // we should have 4 folders in our working folder now.
    ArrayList<String> list = new ArrayList<String>();
    list.add(shards.get(0).getShardName());
    node.undeployShards(list);
    assertEquals(3, workingFolder.list().length);
  }

  private class QueryClient implements Callable<HitsMapWritable> {

    private Node _node;
    private QueryWritable _query;
    private DocumentFrequenceWritable _freqs;
    private String[] _shards;

    public QueryClient(Node node, DocumentFrequenceWritable freqs, QueryWritable query, String[] shards) {
      _node = node;
      _freqs = freqs;
      _query = query;
      _shards = shards;
    }

    @Override
    public HitsMapWritable call() throws Exception {
      return _node.search(_query, _freqs, _shards, 2);
    }

  }

  //
  // public void testCommunication() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final Query query = new Query("foo: bar");
  //
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard1 = new AssignedShard("bla2",
  // "src/test/testIndexA/bIndex");
  // searchServer.addShard(shard1);
  // final DocumentFrequenceWritable docFreqs =
  // searchServer.getDocFreqs(query,
  // new String[] { shard1.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // searchServer.search(query, new String[] { shard1.getName() });
  // RPC.stopClient();
  // client.showFolders(System.out);
  // node.shutdown();
  // Thread.sleep(10000);
  // client.showFolders(System.out);
  // client.close();
  // server.shutdown();
  //
  // }
  //
  // public void testRemoveAndAdd() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Query query = new Query("foo: bar");
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/bIndex");
  // searchServer.addShard(shard);
  // DocumentFrequenceWritable docFreqs = searchServer.getDocFreqs(query, new
  // String[] { shard.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // HitsMapWritable searchHits = searchServer.search(new Query("foo: bar"),
  // new
  // String[] { shard.getName() });
  // Hits hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(1, hits.getHits().size());
  //
  // searchServer.removeShard(shard);
  // docFreqs = searchServer.getDocFreqs(query, new String[] { shard.getName()
  // });
  // docFreqs = searchServer.getDocFreqs(query, new String[] {});
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // searchHits = searchServer.search(query, new String[] { shard.getName()
  // });
  // hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(0, hits.getHits().size());
  //
  // shard = new AssignedShard("bla2", "src/test/testIndexA/aIndex");
  // searchServer.addShard(shard);
  // docFreqs = searchServer.getDocFreqs(query, new String[] { shard.getName()
  // });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // searchHits = searchServer.search(query, new String[] { shard.getName()
  // });
  // hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(2, hits.getHits().size());
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // public void testAddThreeShards() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // final Query query = new Query("foo: bar");
  //
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/bIndex");
  // searchServer.addShard(shard);
  // DocumentFrequenceWritable docFreqs = searchServer.getDocFreqs(query, new
  // String[] { shard.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // HitsMapWritable searchHits = searchServer.search(query, new String[] {
  // shard.getName() });
  // Hits hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(1, hits.getHits().size());
  //
  // outputHits(hits);
  //
  // final AssignedShard shard2 = new AssignedShard("bla2",
  // "src/test/testIndexA/aIndex");
  // searchServer.addShard(shard2);
  // docFreqs = searchServer.getDocFreqs(query, new String[] {
  // shard.getName(),
  // shard2.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // searchHits = searchServer.search(query, new String[] { shard.getName(),
  // shard2.getName() });
  // hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(3, hits.getHits().size());
  //
  // outputHits(hits);
  //
  // final AssignedShard shard3 = new AssignedShard("bla2",
  // "src/test/testIndexA/cIndex");
  // searchServer.addShard(shard3);
  // docFreqs = searchServer
  // .getDocFreqs(query, new String[] { shard.getName(), shard2.getName(),
  // shard3.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // searchHits = searchServer.search(query, new String[] { shard.getName(),
  // shard2.getName(), shard3.getName() });
  // hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(4, hits.getHits().size());
  //
  // outputHits(hits);
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // public void test2Servers() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node nodeServer1 = startNodeServer();
  // final ISearch searchServer1 = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/bIndex");
  // searchServer1.addShard(shard);
  //
  // final Node nodeServer2 = startNodeServer();
  // final ISearch searchServer2 = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20001), new Configuration());
  // final AssignedShard shard2 = new AssignedShard("bla2",
  // "src/test/testIndexA/aIndex");
  // searchServer2.addShard(shard2);
  //
  // final Query query = new Query("foo: bar");
  //
  // final DocumentFrequenceWritable docFreqs =
  // searchServer1.getDocFreqs(query,
  // new String[] { shard.getName() });
  // final DocumentFrequenceWritable docFreqs2 =
  // searchServer2.getDocFreqs(query, new String[] { shard2.getName() });
  // docFreqs.putAll(docFreqs2.getAll());
  // docFreqs.addNumDocs(docFreqs2.getNumDocs());
  // searchServer1.setSimilarityDocFreqs(docFreqs);
  // searchServer2.setSimilarityDocFreqs(docFreqs);
  //
  // final HitsMapWritable searchHits1 = searchServer1.search(query, new
  // String[] { shard.getName() });
  // final Hits hits1 = searchHits1.getHits();
  // final HitsMapWritable searchHits2 = searchServer2.search(query, new
  // String[] { shard2.getName() });
  // final Hits hits2 = searchHits2.getHits();
  //
  // outputHits(hits1);
  // outputHits(hits2);
  //
  // RPC.stopClient();
  // nodeServer1.shutdown();
  // nodeServer2.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // private void outputHits(Hits hits) {
  // for (final Hit hit : hits.getHits()) {
  // Logger.info(hit.getNode() + " -- " + hit.getShard() + " -- " +
  // hit.getDocId() + " -- "
  // + hit.getScore());
  // }
  // }
  //
  // public void testSearchInHadoopApacheOrg() throws IOException,
  // ParseException, InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/dIndex");
  // searchServer.addShard(shard);
  //
  // final Query query = new Query("content: the");
  // final DocumentFrequenceWritable docFreqs =
  // searchServer.getDocFreqs(query,
  // new String[] { shard.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // final HitsMapWritable searchHits = searchServer.search(query, new
  // String[]
  // { shard.getName() });
  // final Hits hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(937, hits.size());
  // assertEquals(937, hits.getHits().size());
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // public void testGetDtails() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/dIndex");
  // searchServer.addShard(shard);
  //
  // final Query query = new Query("content: the");
  // final DocumentFrequenceWritable docFreqs =
  // searchServer.getDocFreqs(query,
  // new String[] { shard.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // final HitsMapWritable searchHits = searchServer.search(query, new
  // String[]
  // { shard.getName() }, 10);
  // final Hits hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(937, hits.size());
  // List<Hit> hits2 = hits.getHits();
  // assertEquals(10, hits2.size());
  // for (Hit hit : hits2) {
  // MapWritable details = searchServer.getDetails(hit.getShard(),
  // hit.getDocId());
  // assertNotNull(details);
  // Writable writable = details.get(new Text("path"));
  // assertNotNull(writable);
  // assertTrue(writable.toString().length() > 0);
  // }
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // public void testGetResultCount() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/dIndex");
  // searchServer.addShard(shard);
  //
  // final Query query = new Query("content: the");
  // final IntWritable count = searchServer.getResultCount(query, new String[]
  // {
  // shard.getName() });
  // assertNotNull(count);
  // assertEquals(937, count.get());
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //
  // public void testSearchRange() throws IOException, ParseException,
  // InterruptedException {
  // final ZkConfiguration conf = new ZkConfiguration();
  // final ZKClient client = new ZKClient(conf);
  // final ZkServer server = new ZkServer(conf);
  // Thread.sleep(3000);
  // if (client.exists(IPaths.ROOT_PATH)) {
  // client.deleteRecursiv(IPaths.ROOT_PATH);
  // }
  // server.startMasterOrNode(client, true);
  //
  // final Node node = startNodeServer();
  // final ISearch searchServer = (ISearch) RPC.getProxy(ISearch.class, 0L,
  // new
  // InetSocketAddress(NetworkUtil
  // .getLocalhostName(), 20000), new Configuration());
  // final AssignedShard shard = new AssignedShard("bla2",
  // "src/test/testIndexA/dIndex");
  // searchServer.addShard(shard);
  //
  // final Query query = new Query("content: the");
  // final DocumentFrequenceWritable docFreqs =
  // searchServer.getDocFreqs(query,
  // new String[] { shard.getName() });
  // searchServer.setSimilarityDocFreqs(docFreqs);
  // final HitsMapWritable searchHits = searchServer.search(query, new
  // String[]
  // { shard.getName() }, 37);
  // final Hits hits = searchHits.getHits();
  // assertNotNull(hits);
  // assertEquals(937, hits.size());
  // assertEquals(37, hits.getHits().size());
  //
  // RPC.stopClient();
  // node.shutdown();
  // Thread.sleep(3000);
  // client.close();
  // server.shutdown();
  // }
  //

}
