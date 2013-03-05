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

import static org.junit.Assert.*;

import static org.mockito.Matchers.*;

import static org.mockito.Mockito.*;

import static org.fest.assertions.Assertions.*;

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
import net.sf.katta.lib.lucene.LuceneServer.SearcherHandle;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.mockito.ChainedAnswer;
import net.sf.katta.testutil.mockito.PauseAnswer;
import net.sf.katta.testutil.mockito.SleepingAnswer;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class LuceneServerTest extends AbstractTest {

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

  @Test
  public void testSearchCall_EmptyIndex() throws Exception {
    IndexSearcher searcher = mock(IndexSearcher.class);
    when(searcher.maxDoc()).thenReturn(0);

    Weight weight = mock(Weight.class);

    LuceneServer server = mock(LuceneServer.class);
    when(server.getSearcherHandleByShard("testShard")).thenReturn(new SearcherHandle(searcher));

    LuceneServer.SearchCall searchCall = server.new SearchCall("testShard", weight, 10, null, 1000, 1, null);
    LuceneServer.SearchResult result = searchCall.call();
    assertThat(result._totalHits).as("results").isEqualTo(0);
  }

  @Test
  public void testSearchDuringUndeploy() throws Exception {
    final String[] shardToUndeploy = new String[1];

    /*
     * Create a special LuceneServer that pauses on a call to maxDoc(). Here,
     * maxDoc() is called only from SearchCall within LuceneServer, right before
     * performing the real search.
     */
    final PauseAnswer<Void> pauseAnswer = new PauseAnswer<Void>(null);
    final LuceneServer server = new LuceneServer("ls", new DefaultSearcherFactory(), 0.75f) {
      @Override
      protected SearcherHandle getSearcherHandleByShard(String shardName) {
        SearcherHandle handle;
        if (shardName.equals(shardToUndeploy[0])) {
          handle = spy(super.getSearcherHandleByShard(shardName));
          Answer<IndexSearcher> answer = new Answer<IndexSearcher>() {
            @Override
            public IndexSearcher answer(InvocationOnMock invocation) throws Throwable {
              IndexSearcher searcher = spy((IndexSearcher) invocation.callRealMethod());
              doAnswer(new ChainedAnswer(pauseAnswer, new CallsRealMethods())).when(searcher).maxDoc();
              return searcher;
            }
          };
          doAnswer(answer).when(handle).getSearcher();
        } else {
          handle = super.getSearcherHandleByShard(shardName);
        }

        return handle;
      }
    };
    final String[] shardNames = addIndexShards(server, TestResources.INDEX1);
    shardToUndeploy[0] = shardNames[0];

    final QueryWritable writable = new QueryWritable(new TermQuery(new Term("foo", "bar")));
    final DocumentFrequencyWritable freqs = server.getDocFreqs(writable, shardNames);

    // Storage for the result or exception from the search call
    final HitsMapWritable[] result = new HitsMapWritable[1];
    final Exception[] exception = new Exception[1];

    // Run the query in another thread
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          result[0] = server.search(writable, freqs, shardNames, 10000);
        } catch (Exception e) {
          exception[0] = e;
        }
      }
    };
    t.start();

    // Wait until the search gets to the SearchCall, where it will remain paused
    pauseAnswer.joinExecutionBegin();

    // Get the searcher so it can be checked that it's really closed
    LuceneServer.SearcherHandle handle = server.getSearcherHandleByShard(shardNames[0]);
    IndexSearcher searcher = handle.getSearcher();
    // ... decrement the ref count though
    handle.finishSearcher();

    // Undeploy the shard (in another thread)
    Thread t2 = new Thread() {
      @Override
      public void run() {
        server.removeShard(shardNames[0]);
      }
    };
    t2.start();

    // Resume the SearchCall thread, then wait for the search thread to finish
    pauseAnswer.resumeExecution(true);
    t.join();
    t2.join();

    // Fail the test if there was an exception
    if (exception[0] != null) {
      throw exception[0];
    }

    assertThat(result[0].getTotalHits()).as("Results returned from search").isEqualTo(4);
    try {
      // Expected: java.lang.IllegalStateException: no index-server for shard
      // 'aIndex' found - probably undeployed
      server.getSearcherHandleByShard(shardNames[0]);
      fail("IllegalStateException not thrown when trying to get undeployed shard handle");
    } catch (IllegalStateException e) {
    }
    try {
      // Expected: org.apache.lucene.store.AlreadyClosedException: this
      // IndexReader is closed
      searcher.doc(0);
      fail("AlreadyClosedException not thrown when trying to access closed index");
    } catch (AlreadyClosedException e) {
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
