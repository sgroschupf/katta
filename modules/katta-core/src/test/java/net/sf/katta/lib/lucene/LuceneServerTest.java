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
package net.sf.katta.lib.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import net.sf.katta.AbstractTest;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.mockito.ChainedAnswer;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class LuceneServerTest extends AbstractTest {

  @Test
  public void testPriorityQueue() throws Exception {
    // tests some simple PriorityQueue behavior
    LuceneServer.KattaHitQueue queue = new LuceneServer.KattaHitQueue(2);
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

  @Test
  public void testPriorityQueue_sameScore() throws Exception {
    LuceneServer.KattaHitQueue queue = new LuceneServer.KattaHitQueue(2);
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

  @Test
  public void testConfiguration() throws Exception {
    // no property in configuration
    LuceneServer server = new LuceneServer();
    server.init("server", newNodeConfiguration());
    assertEquals("server", server.getNodeName());
    assertEquals(0.75f, server.getTimeoutPercentage(), 0.5);
    server.shutdown();

    // property in configuration
    server = new LuceneServer();
    server.init("server", newNodeConfiguration(LuceneServer.CONF_KEY_COLLECTOR_TIMOUT_PERCENTAGE, "0.5"));
    assertEquals("server", server.getNodeName());
    assertEquals(0.5f, server.getTimeoutPercentage(), 0.5);
  }

  @Test
  public void testSearch_Timeout() throws Exception {
    int clientTiemout = 10000;
    // disabled timeout
    LuceneServer server = new LuceneServer("server", new DefaultSearcherFactory(), 0.0f);
    String[] shardNames = addIndexShards(server, TestResources.INDEX1);

    QueryWritable queryWritable = new QueryWritable(parseQuery("foo: b*"));
    DocumentFrequencyWritable freqs = server.getDocFreqs(queryWritable, shardNames);
    HitsMapWritable result = server.search(queryWritable, freqs, shardNames, clientTiemout, 1000);
    assertEquals(4, result.getHitList().size());
    server.shutdown();

    // timeout - success
    server = new LuceneServer("server", new DefaultSearcherFactory(), 0.5f);
    addIndexShards(server, TestResources.INDEX1);
    freqs = server.getDocFreqs(queryWritable, shardNames);
    result = server.search(queryWritable, freqs, shardNames, clientTiemout, 1000);
    assertEquals(4, result.getHitList().size());
    server.shutdown();

    // timeout - failure
    final long serverTimeout = 100;
    final DefaultSearcherFactory seacherFactory = new DefaultSearcherFactory();
    ISeacherFactory mockSeacherFactory = mock(ISeacherFactory.class);
    final AtomicInteger shardsWithTiemoutCount = new AtomicInteger();
    when(mockSeacherFactory.createSearcher(anyString(), any(File.class))).thenAnswer(new Answer<IndexSearcher>() {
      @Override
      public IndexSearcher answer(InvocationOnMock invocation) throws Throwable {
        final IndexSearcher indexSearcher = seacherFactory.createSearcher((String) invocation.getArguments()[0],
                (File) invocation.getArguments()[1]);
        synchronized (shardsWithTiemoutCount) {
          if (shardsWithTiemoutCount.intValue() >= 2) {
            // 2 from 4 shards will get tiemout
            return indexSearcher;
          }
          shardsWithTiemoutCount.incrementAndGet();
        }
        IndexSearcher indexSearcherSpy = spy(indexSearcher);
        doAnswer(new ChainedAnswer(new SleepingAnswer(serverTimeout * 2), new CallsRealMethods())).when(
                indexSearcherSpy).search(any(Weight.class), any(Filter.class), any(Collector.class));
        return indexSearcherSpy;
      }
    });
    server = new LuceneServer("server", mockSeacherFactory, 0.01f);
    assertEquals(serverTimeout, server.getCollectorTiemout(clientTiemout));
    addIndexShards(server, TestResources.INDEX1);
    freqs = server.getDocFreqs(queryWritable, shardNames);
    result = server.search(queryWritable, freqs, shardNames, clientTiemout, 1000);
    assertTrue(result.getHitList().size() < 4);
    assertTrue(result.getHitList().size() >= 1);
    server.shutdown();
  }

  private String[] addIndexShards(LuceneServer server, File index) throws IOException {
    File[] shards = index.listFiles();
    String[] shardNames = index.list();
    for (File shard : shards) {
      server.addShard(shard.getName(), shard);
    }
    return shardNames;
  }

  @Test
  public void testSearch_MultiThread() throws Exception {
    LuceneServer server = new LuceneServer("ls", new DefaultSearcherFactory(), 0.75f);
    String[] shardNames = addIndexShards(server, TestResources.INDEX1);

    QueryWritable writable = new QueryWritable(parseQuery("foo: bar"));
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
        float lastScore = last.getHitList().get(0).getScore();
        float currentScore = hitsMapWritable.getHitList().get(0).getScore();
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
      return _server.search(_query, _freqs, _shards, 10000, 2);
    }

  }
}
