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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import net.sf.katta.AbstractTest;
import net.sf.katta.lib.lucene.query.ILuceneQueryAndFilterWritable;
import net.sf.katta.lib.lucene.query.PrefixQueryWritable;
import net.sf.katta.lib.lucene.query.TermQueryWritable;
import net.sf.katta.testutil.LuceneTestResources;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.mockito.ChainedAnswer;
import net.sf.katta.testutil.mockito.PauseAnswer;
import net.sf.katta.testutil.mockito.SleepingAnswer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.junit.Test;
import org.mockito.Matchers;
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
    String[] shardNames = addIndexShards(server, LuceneTestResources.INDEX1);

    ILuceneQueryAndFilterWritable queryWritable = new PrefixQueryWritable("foo", "b");
    DocumentFrequencyWritable freqs = server.getDocFreqs(queryWritable, shardNames);
    HitsMapWritable result = server.search(queryWritable, freqs, shardNames, clientTiemout, 1000);
    assertEquals(4, result.getHitList().size());
    server.shutdown();

    // timeout - success
    server = new LuceneServer("server", new DefaultSearcherFactory(), 0.5f);
    addIndexShards(server, LuceneTestResources.INDEX1);
    freqs = server.getDocFreqs(queryWritable, shardNames);
    result = server.search(queryWritable, freqs, shardNames, clientTiemout, 1000);
    assertEquals(4, result.getHitList().size());
    server.shutdown();

    // timeout - failure
    final long serverTimeout = 100;
    final DefaultSearcherFactory seacherFactory = new DefaultSearcherFactory();
    ISeacherFactory mockSeacherFactory = mock(ISeacherFactory.class);
    final AtomicInteger shardsWithTimeoutCount = new AtomicInteger();
    Answer<IndexHandle> answer = new Answer<IndexHandle>() {
      @Override
      public IndexHandle answer(InvocationOnMock invocation) throws Throwable {
        final IndexHandle indexHandle = seacherFactory.createSearcher((String) invocation.getArguments()[0],
                (File) invocation.getArguments()[1]);
        synchronized (shardsWithTimeoutCount) {
          if (shardsWithTimeoutCount.intValue() >= 1) {
            // 2 from 3 shards will get tiemout
            return indexHandle;
          }
          shardsWithTimeoutCount.incrementAndGet();
        }
        IndexHandle indexHandleSpy = spy(indexHandle);
        final IndexSearcher searcherSpy = spy(indexHandleSpy.getSearcher());
        try {
          doAnswer(new ChainedAnswer(new SleepingAnswer(serverTimeout * 2), new CallsRealMethods())).
              when(searcherSpy).search(any(Query.class), any(Filter.class), any(Collector.class));
        } finally {
          indexHandleSpy.finishSearcher();
        }
        doAnswer(new Answer<IndexSearcher>() {
          public IndexSearcher answer(InvocationOnMock invocation) throws Throwable {
            indexHandle.getSearcher();
            return searcherSpy;
          }
        }).when(indexHandleSpy).getSearcher();

        return indexHandleSpy;
      }
    };
    when(mockSeacherFactory.createSearcher(anyString(), any(File.class))).thenAnswer(answer);
    doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) throws Throwable {
        IndexHandle indexHandle = (IndexHandle) invocation.getArguments()[0];
        indexHandle.closeSearcher();
        return null;
      }
    }).when(mockSeacherFactory).closeSearcher(any(IndexHandle.class));
    server = new LuceneServer("server", mockSeacherFactory, 0.01f);
    assertEquals(serverTimeout, server.getCollectorTiemout(clientTiemout));
    addIndexShards(server, LuceneTestResources.INDEX1);
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
    String[] shardNames = addIndexShards(server, LuceneTestResources.INDEX1);

    ILuceneQueryAndFilterWritable queryAndFilter = new TermQueryWritable("foo", "bar");
    DocumentFrequencyWritable freqs = server.getDocFreqs(queryAndFilter, shardNames);

    ExecutorService es = Executors.newFixedThreadPool(100);
    List<Future<HitsMapWritable>> tasks = new ArrayList<Future<HitsMapWritable>>();
    for (int i = 0; i < 10000; i++) {
      QueryClient client = new QueryClient(server, freqs, queryAndFilter, shardNames);
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
    IndexReader reader = mock(IndexReader.class);
    when(reader.maxDoc()).thenReturn(0);

    IndexSearcher searcher = mock(IndexSearcher.class);
    when(searcher.getIndexReader()).thenReturn(reader);

    Query query = mock(Query.class);

    LuceneServer server = mock(LuceneServer.class);
    when(server.getSearcherHandleByShard("testShard")).thenReturn(new IndexHandle(reader, searcher));

    LuceneServer.SearchCall searchCall = server.new SearchCall("testShard", query, Collections
        .<TermWritable, Integer>emptyMap(), 1, 10, null, 1000, 1, null);
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
      protected IndexHandle getSearcherHandleByShard(String shardName) {
        IndexHandle handle;
        if (shardName.equals(shardToUndeploy[0])) {
          handle = spy(super.getSearcherHandleByShard(shardName));

          Answer<IndexSearcher> answer = new Answer<IndexSearcher>() {
            @Override
            public IndexSearcher answer(InvocationOnMock invocation) throws Throwable {
              IndexSearcher searcher = spy((IndexSearcher) invocation.callRealMethod());

              //when(reader.maxDoc()).thenAnswer(new ChainedAnswer(pauseAnswer, new CallsRealMethods()));
              doAnswer(new ChainedAnswer(pauseAnswer, new CallsRealMethods())).when(searcher).search(
                  Matchers.<Query>any(), Matchers.<Filter>any(), Matchers.<Collector>any());

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
    final String[] shardNames = addIndexShards(server, LuceneTestResources.INDEX1);
    shardToUndeploy[0] = shardNames[0];

    final ILuceneQueryAndFilterWritable writable = new TermQueryWritable("foo", "bar");


    // Storage for the result or exception from the search call
    final HitsMapWritable[] result = new HitsMapWritable[1];
    final Exception[] exception = new Exception[1];

    final Object lock1 = new Object();
    final Object lock2 = new Object();
    // Run the query in another thread
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          DocumentFrequencyWritable freqs;
          freqs = server.getDocFreqs(writable, shardNames);

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
    IndexHandle handle = server.getSearcherHandleByShard(shardNames[0]);
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
    private ILuceneQueryAndFilterWritable _queryAndFilter;
    private DocumentFrequencyWritable _freqs;
    private String[] _shards;

    public QueryClient(LuceneServer server, DocumentFrequencyWritable freqs, ILuceneQueryAndFilterWritable queryAndFilter, String[] shards) {
      _server = server;
      _freqs = freqs;
      _queryAndFilter = queryAndFilter;
      _shards = shards;
    }

    @Override
    public HitsMapWritable call() throws Exception {
      return _server.search(_queryAndFilter, _freqs, _shards, 10000, 2);
    }

  }
}
