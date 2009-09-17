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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.node.Hits;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.StringUtil;
import net.sf.katta.util.ZkConfiguration;

import org.apache.log4j.Logger;

public class SearchIntegrationTest extends TestCase {

  private KattaMiniCluster _miniCluster;

  @Override
  protected void tearDown() throws Exception {
    if (_miniCluster != null) {
      _miniCluster.stop();
    }
    super.tearDown();
  }
  
  public void testSearch() throws Exception {
    long startTime = System.currentTimeMillis();
    _miniCluster = startMiniCluster(5, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    // stop everything
    int queryTime = 20000;
    Thread.sleep(queryTime);
    searchThread.interrupt();
    searchThread.join();

    checkResults(startTime, queryTime, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
            searchThread.getThrownExceptions());
  }

  public void testMultithreadedSearchWithMultipleClients() throws Exception {
    long startTime = System.currentTimeMillis();
    _miniCluster = startMiniCluster(5, 3, 3);

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
    List<Exception> exceptions = new ArrayList<Exception>();
    for (SearchThread searchThread : searchThreads) {
      searchThread.interrupt();
      searchThread.join();
      firedQueries += searchThread.getFiredQueryCount();
      unexpectedResultCount += searchThread.getUnexpectedResultCount();
      exceptions.addAll(searchThread.getThrownExceptions());
    }

    checkResults(startTime, queryTime, firedQueries, unexpectedResultCount, exceptions);
  }

  public void testSearchWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();
    _miniCluster = startMiniCluster(3, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread searchThread = new SearchThread(expectedHitCount);
    searchThread.start();

    long queryTime = 20000;

    startAndStopNodes(queryTime);

    // stop everything
    searchThread.interrupt();
    searchThread.join();
    _miniCluster.stop();

    checkResults(startTime, queryTime, searchThread.getFiredQueryCount(), searchThread.getUnexpectedResultCount(),
            searchThread.getThrownExceptions());
  }

  private void startAndStopNodes(long queryTime) throws InterruptedException {
    Node node1 = _miniCluster.getNode(0);
    Node node2 = _miniCluster.getNode(1);

    assertNotNull(node1);
    assertNotNull(node2);
    
    Thread.sleep(queryTime / 4);
    node1.shutdown();
    
    Thread.sleep(queryTime / 4);
    node2.getRpcServer().stop();
    
    Thread.sleep(queryTime / 4);
    NodeConfiguration nodeConf = new NodeConfiguration();
    nodeConf.setShardFolder(new File(nodeConf.getShardFolder(), "-new").getAbsolutePath());
    Node newNode = new Node(_miniCluster.getZkConfiguration(), _miniCluster.getZkClient(), nodeConf, new LuceneServer());
    newNode.start();

    Thread.sleep(queryTime / 4);

    newNode.shutdown();
  }

  public void testMultithreadedSearchWithOneClientWhileStartingAndStoppingNodes() throws Exception {
    long startTime = System.currentTimeMillis();
    _miniCluster = startMiniCluster(3, 3, 3);

    // start search threads
    int expectedHitCount = 12;
    SearchThread[] searchThreads = new SearchThread[25];
    ILuceneClient searchClient = new LuceneClient();
    for (int i = 0; i < searchThreads.length; i++) {
      searchThreads[i] = new SearchThread(searchClient, expectedHitCount);
      searchThreads[i].start();
    }

    long queryTime = 20000;
    startAndStopNodes(queryTime);

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

    checkResults(startTime, queryTime, firedQueries, unexpectedResultCount, exceptions);
  }

  private void checkResults(long startTime, long queryTime, long firedQueries, long unexpectedResultCount,
          List<Exception> exceptions) throws Exception {
    // print results
    System.out.println("===========================================");
    System.out.println("Results of " + getName() + ":");
    System.out.println("search time: " + StringUtil.formatTimeDuration(queryTime));
    System.out.println("fired queries: " + firedQueries);
    System.out.println("wrong results: " + unexpectedResultCount);
    System.out.println("exceptions: " + exceptions.size());
    System.out.println("execution took: " + StringUtil.formatTimeDuration(System.currentTimeMillis() - startTime));
    System.out.println("===========================================");

    // assert results
    if (!exceptions.isEmpty()) {
      throw new IllegalStateException("Found exception.", exceptions.get(0));
    }
    assertEquals("wrong hit count", 0, unexpectedResultCount);
  }

  private KattaMiniCluster startMiniCluster(int nodeCount, int indexCount, int replicationCount) throws KattaException,
      InterruptedException {
    ZkConfiguration conf = new ZkConfiguration();
    FileUtil.deleteFolder(new File(conf.getZKDataDir()));
    FileUtil.deleteFolder(new File(conf.getZKDataLogDir()));
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
