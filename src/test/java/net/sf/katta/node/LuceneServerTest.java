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
import junit.framework.TestCase;
import net.sf.katta.node.DocumentFrequencyWritable;
import net.sf.katta.node.Hit;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.QueryWritable;
import net.sf.katta.node.LuceneServer.KattaHitQueue;
import net.sf.katta.testutil.TestResources;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

public class LuceneServerTest extends TestCase {

  public void testPriorityQueue() throws Exception {
    // tests some simple PriorityQueue behavior
    LuceneServer.KattaHitQueue queue = new LuceneServer().new KattaHitQueue(2);
    Hit hit1 = new Hit("shard", "node", 1f, 1);
    Hit hit2 = new Hit("shard", "node", 2f, 1);
    Hit hit3 = new Hit("shard", "node", 3f, 1);
    Hit hit4 = new Hit("shard", "node", 4f, 1);

    assertTrue(queue.insert(hit1));
    assertTrue(queue.insert(hit2));
    assertTrue(queue.insert(hit3));
    assertTrue(queue.insert(hit4));

    assertEquals(2, queue.size());
    assertSame(hit3, queue.pop());
    assertSame(hit4, queue.pop());
  }

  public void testPriorityQueue_sameScore() throws Exception {
    LuceneServer.KattaHitQueue queue = new LuceneServer().new KattaHitQueue(2);
    Hit hit1 = new Hit("shard", "node", 1f, 1);
    Hit hit2 = new Hit("shard", "node", 1f, 2);
    Hit hit3 = new Hit("shard", "node", 1f, 3);

    assertTrue(queue.insert(hit1));
    assertTrue(queue.insert(hit2));
    assertTrue(queue.insert(hit3));
    assertEquals(2, queue.size());

    // Queue should return documents with the smaller document ids first if
    // documents have the same score.
    assertSame(hit2, queue.pop());
    assertSame(hit3, queue.pop());
  }

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

  public void testMultiThreadSearch() throws Exception {
    LuceneServer server = new LuceneServer();
    File[] shards = TestResources.INDEX1.listFiles();
    String[] shardNames = TestResources.INDEX1.list();
    for (File shard : shards) {
      server.addShard(shard.getName(), shard);
    }

    QueryParser parser = new QueryParser("field", new KeywordAnalyzer());
    Query query = parser.parse("foo: bar");
    QueryWritable writable = new QueryWritable(query);

    DocumentFrequencyWritable freqs = server.getDocFreqs(writable, shardNames);

    ExecutorService es = Executors.newFixedThreadPool(100);
    List<Future<HitsMapWritable>> tasks = new ArrayList<Future<HitsMapWritable>>();
    for (int i = 0; i < 10000; i++) {
      QueryClient client = new QueryClient(server, freqs, writable, shardNames);
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

  private static class QueryClient implements Callable<HitsMapWritable> {

    private LuceneServer _server;
    private QueryWritable _query;
    private DocumentFrequencyWritable _freqs;
    private String[] _shards;

    public QueryClient(LuceneServer server, DocumentFrequencyWritable freqs, QueryWritable query, String[] shards) {
      _server = server;
      _freqs = freqs;
      _query = query;
      _shards = shards;
    }

    @Override
    public HitsMapWritable call() throws Exception {
      return _server.search(_query, _freqs, _shards, 2);
    }

  }
}
