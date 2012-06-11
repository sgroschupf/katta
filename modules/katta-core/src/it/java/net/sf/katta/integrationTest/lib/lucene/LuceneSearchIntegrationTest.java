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
package net.sf.katta.integrationTest.lib.lucene;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.client.Client;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.lib.lucene.Hits;
import net.sf.katta.lib.lucene.ILuceneClient;
import net.sf.katta.lib.lucene.ILuceneServer;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.util.StringUtil;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class LuceneSearchIntegrationTest extends AbstractIntegrationTest {

  private static final int SEARCH_THREAD_COUNT = 9;
  private static final int QUERY_TIME = 5000;

  public LuceneSearchIntegrationTest() {
    super(5, false, false);
  }

  @Override
  protected void afterClusterStart() throws Exception {
    deployTestIndices(3, 3);
  }

  @Test
  public void testSearch() throws Exception {
    long startTime = System.currentTimeMillis();

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    Thread.sleep(QUERY_TIME);
    searchThread.interrupt();
    searchThread.join();

    checkResults(startTime, QUERY_TIME, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
            searchThread.getThrownExceptions());
  }

  @Test
  public void testMultithreadedSearchWithMultipleClients() throws Exception {
    long startTime = System.currentTimeMillis();

    // start search threads
    int expectedHitCount = 12;
    SearchThread[] searchThreads = new SearchThread[SEARCH_THREAD_COUNT];
    for (int i = 0; i < searchThreads.length; i++) {
      searchThreads[i] = new SearchThread(expectedHitCount);
      searchThreads[i].start();
    }
    Thread.sleep(QUERY_TIME);

    // stop everything
    long firedQueries = 0;
    long unexpectedResultCount = 0;
    List<Exception> exceptions = new ArrayList<Exception>();
    for (SearchThread searchThread : searchThreads) {
      searchThread.interrupt();
      searchThread.join();
      firedQueries += searchThread.getFiredQueryCount();
      unexpectedResultCount += searchThread.getUnexpectedResultCount();
      exceptions.addAll(searchThread.getThrownExceptions());
    }

    checkResults(startTime, QUERY_TIME, firedQueries, unexpectedResultCount, exceptions);
  }

  @Test
  public void testSearchWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    startAndStopNodes(QUERY_TIME);

    // stop everything
    searchThread.interrupt();
    searchThread.join();
    checkResults(startTime, QUERY_TIME, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
            searchThread.getThrownExceptions());
  }

  @Test
  public void testMultithreadedSearchWithOneClientWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();

    // start search threads
    int expectedHitCount = 12;
    SearchThread[] searchThreads = new SearchThread[SEARCH_THREAD_COUNT];
    ILuceneClient searchClient = new LuceneClient();
    for (int i = 0; i < searchThreads.length; i++) {
      searchThreads[i] = new SearchThread(searchClient, expectedHitCount);
      searchThreads[i].start();
    }

    startAndStopNodes(QUERY_TIME);

    // stop everything
    long firedQueries = 0;
    long unexpectedResultCount = 0;
    List<Exception> exceptions = new ArrayList<Exception>();
    for (SearchThread searchThread : searchThreads) {
      searchThread.interrupt();
      searchThread.join();
      firedQueries += searchThread.getFiredQueryCount();
      unexpectedResultCount += searchThread.getUnexpectedResultCount();
      exceptions.addAll(searchThread.getThrownExceptions());
    }
    searchClient.close();

    checkResults(startTime, QUERY_TIME, firedQueries, unexpectedResultCount, exceptions);
  }

  private void startAndStopNodes(long queryTime) throws Exception {
    Client client = new Client(ILuceneServer.class, _protocol);
    Thread.sleep(queryTime / 4);
    System.out.println("-----------------------SHUTDOWN NODE1: "
            + _protocol.getShard2NodesMap(_protocol.getShard2NodeShards()));
    System.out.println(client.getSelectionPolicy().toString());
    _miniCluster.shutdownNode(0);

    Thread.sleep(queryTime / 4);
    System.out.println("-----------------------SHUTDOWN NODE2: "
            + _protocol.getShard2NodesMap(_protocol.getShard2NodeShards()));
    System.out.println(client.getSelectionPolicy().toString());
    _miniCluster.shutdownNode(0);

    Thread.sleep(queryTime / 4);
    System.out.println("-----------------------START NEW NODE: "
            + _protocol.getShard2NodesMap(_protocol.getShard2NodeShards()));
    System.out.println(client.getSelectionPolicy().toString());
    _miniCluster.startAdditionalNode();

    System.out.println("-----------------------SHUTDOWN NEW NODE: "
            + _protocol.getShard2NodesMap(_protocol.getShard2NodeShards()));
    System.out.println(client.getSelectionPolicy().toString());
    Thread.sleep(queryTime / 2);
    _miniCluster.shutdownNode(_miniCluster.getRunningNodeCount() - 1);
  }

  private void checkResults(long startTime, long queryTime, long firedQueries, long unexpectedResultCount,
          List<Exception> exceptions) throws Exception {
    // print results
    System.out.println("===========================================");
    System.out.println("Results of " + _printMethodNames.getCurrentMethodName() + ":");
    System.out.println("search time: " + StringUtil.formatTimeDuration(queryTime));
    System.out.println("fired queries: " + firedQueries);
    System.out.println("wrong results: " + unexpectedResultCount);
    System.out.println("exceptions: " + exceptions.size());
    System.out.println("execution took: " + StringUtil.formatTimeDuration(System.currentTimeMillis() - startTime));
    System.out.println("===========================================");

    // assert results
    if (!exceptions.isEmpty()) {
      throw new IllegalStateException(exceptions.size() + " exception during search", exceptions.get(0));
    }
    assertEquals("wrong hit count", 0, unexpectedResultCount);
  }

  protected static class SearchThread extends Thread {

    private static Logger LOG = Logger.getLogger(SearchThread.class);

    private volatile boolean _stopped;
    private final long _expectedTotalHitCount;

    private List<Exception> _thrownExceptions = new ArrayList<Exception>();
    private long _firedQueryCount;
    private long _unexpectedResultCount;

    private ILuceneClient _client;

    public SearchThread(long expectedTotalHitCount) {
      _expectedTotalHitCount = expectedTotalHitCount;
    }

    public SearchThread(ILuceneClient client, long expectedTotalHitCount) {
      _client = client;
      _expectedTotalHitCount = expectedTotalHitCount;
    }

    @Override
    public void run() {
      try {
        ILuceneClient client;
        if (_client == null) {
          client = new LuceneClient();
        } else {
          client = _client;
        }
        while (!_stopped) {
          final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("foo:bar");
          Hits hits = client.search(query, new String[] { "*" });
          _firedQueryCount++;
          if (hits.size() != _expectedTotalHitCount) {
            _unexpectedResultCount++;
            LOG.error("expected " + _expectedTotalHitCount + " hits but got " + hits.size());
          }
        }
        if (_client == null) {
          client.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
        _thrownExceptions.add(e);
      }
    }

    @Override
    public void interrupt() {
      _stopped = true;
      // jz: we don't call super.interrupt() since the client swallows such
      // InterruptedException's
    }

    public List<Exception> getThrownExceptions() {
      return _thrownExceptions;
    }

    public long getFiredQueryCount() {
      return _firedQueryCount;
    }

    public long getUnexpectedResultCount() {
      return _unexpectedResultCount;
    }

  }
}
