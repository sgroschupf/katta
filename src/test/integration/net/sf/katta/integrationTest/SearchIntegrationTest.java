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
package net.sf.katta.integrationTest;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.StringUtil;
import net.sf.katta.util.ZkConfiguration;

import org.apache.log4j.Logger;

public class SearchIntegrationTest extends TestCase {

  public void testSearch() throws Exception {
    long startTime = System.currentTimeMillis();
    KattaMiniCluster miniCluster = startMiniCluster(5, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    // stop everything
    int queryTime = 20000;
    Thread.sleep(queryTime);
    searchThread.interrupt();
    searchThread.join();
    miniCluster.stop();

    checkResults(startTime, queryTime, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
        searchThread.getThrownExceptions().size());
  }

  public void testMultithreadedSearchWithMultipleClients() throws Exception {
    long startTime = System.currentTimeMillis();
    KattaMiniCluster miniCluster = startMiniCluster(5, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread[] searchThreads = new SearchThread[25];
    for (int i = 0; i < searchThreads.length; i++) {
      searchThreads[i] = new SearchThread(expectedHitCount);
      searchThreads[i].start();
    }
    long queryTime = 20000;
    Thread.sleep(queryTime);

    // stop everything
    long firedQueries = 0;
    long unexpectedResultCount = 0;
    int exceptionCount = 0;
    for (SearchThread searchThread : searchThreads) {
      searchThread.interrupt();
      searchThread.join();
      firedQueries += searchThread.getFiredQueryCount();
      unexpectedResultCount += searchThread.getUnexpectedResultCount();
      exceptionCount += searchThread.getThrownExceptions().size();
    }
    miniCluster.stop();

    checkResults(startTime, queryTime, firedQueries, unexpectedResultCount, exceptionCount);
  }

  public void testSearchWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();
    KattaMiniCluster miniCluster = startMiniCluster(3, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    long queryTime = 20000;
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(0).shutdown();
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(1).getRpcServer().stop();
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(0).start();
    Thread.sleep(queryTime / 4);

    // stop everything
    searchThread.interrupt();
    searchThread.join();
    miniCluster.stop();

    checkResults(startTime, queryTime, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
        searchThread.getThrownExceptions().size());
  }

  public void testMultithreadedSearchWithOneClientWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();
    KattaMiniCluster miniCluster = startMiniCluster(3, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread[] searchThreads = new SearchThread[25];
    ILuceneClient searchClient = new LuceneClient();
    for (int i = 0; i < searchThreads.length; i++) {
      searchThreads[i] = new SearchThread(searchClient, expectedHitCount);
      searchThreads[i].start();
    }

    long queryTime = 20000;
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(0).shutdown();
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(1).getRpcServer().stop();
    Thread.sleep(queryTime / 4);
    miniCluster.getNode(0).start();
    Thread.sleep(queryTime / 4);

    // stop everything
    long firedQueries = 0;
    long unexpectedResultCount = 0;
    int exceptionCount = 0;
    for (SearchThread searchThread : searchThreads) {
      searchThread.interrupt();
      searchThread.join();
      firedQueries += searchThread.getFiredQueryCount();
      unexpectedResultCount += searchThread.getUnexpectedResultCount();
      exceptionCount += searchThread.getThrownExceptions().size();
    }
    searchClient.close();
    miniCluster.stop();

    checkResults(startTime, queryTime, firedQueries, unexpectedResultCount, exceptionCount);
  }

  private void checkResults(long startTime, long queryTime, long firedQueries, long unexpectedResultCount,
      int exceptionCount) {
    // print results
    System.out.println("===========================================");
    System.out.println("Results of " + getName() + ":");
    System.out.println("search time: " + StringUtil.formatTimeDuration(queryTime));
    System.out.println("fired queries: " + firedQueries);
    System.out.println("wrong results: " + unexpectedResultCount);
    System.out.println("exceptions: " + exceptionCount);
    System.out.println("execution took: " + StringUtil.formatTimeDuration(System.currentTimeMillis() - startTime));
    System.out.println("===========================================");

    // assert results
    assertEquals("exceptions on search", 0, exceptionCount);
    assertEquals("wrong hit count", 0, unexpectedResultCount);
    // TODO jz: add assertions
  }

  private KattaMiniCluster startMiniCluster(int nodeCount, int indexCount, int replicationCount) throws KattaException,
      InterruptedException {
    ZkConfiguration conf = new ZkConfiguration();
    FileUtil.deleteFolder(conf.getZKDataDir());
    FileUtil.deleteFolder(conf.getZKDataLogDir());
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());

    // start katta cluster
    KattaMiniCluster miniCluster = new KattaMiniCluster(conf, nodeCount);
    miniCluster.start();

    // deploy indexes
    miniCluster.deployTestIndexes(TestResources.INDEX1, indexCount, replicationCount);
    return miniCluster;
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
          Hits hits = client.search(new Query("foo:bar"), new String[] { "*" });
          _firedQueryCount++;
          if (hits.size() != _expectedTotalHitCount) {
            _unexpectedResultCount++;
            LOG.warn("expected " + _expectedTotalHitCount + " hits but got " + hits.size());
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
