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
package net.sf.katta.loadtest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.Katta;
import net.sf.katta.node.BaseRpcServer;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.LoadTestNodeConfiguration;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class LoadTestNode extends BaseRpcServer implements TestCommandListener {

  final static Logger LOG = Logger.getLogger(LoadTestNode.class);

  private ZKClient _zkClient;
  ScheduledExecutorService _executorService = Executors.newScheduledThreadPool(1);
  private Writer _statisticsWriter;

  private Lock _shutdownLock = new ReentrantLock(true);
  private volatile boolean _shutdown = false;
  LoadTestNodeConfiguration _configuration;
  TestSearcherMetaData _metaData;

  private String _currentNodeName;

  private final class TestSearcherRunnable implements Runnable {
    private int _count;
    private String[] _indexNames;
    private String _queryString;
    private Random _random = new Random(System.currentTimeMillis());

    TestSearcherRunnable(int count, String[] indexNames, String queryString) {
      _count = count;
      _indexNames = indexNames;
      _queryString = queryString;
    }

    @Override
    public void run() {
      // TODO PVo search for different terms
      try {
        long startTime = System.currentTimeMillis();
        Katta.search(_indexNames, _queryString, _count);
        long endTime = System.currentTimeMillis();
        writeStatistics(endTime - startTime);
      } catch (KattaException e) {
        writeStatistics(-1);
        LOG.error("Search failed.", e);
      }
      _executorService.schedule(this, _random.nextInt(_configuration.getTestDelay()), TimeUnit.MILLISECONDS);
    }
  }

  class ReconnectListener implements IZkReconnectListener {

    @Override
    public void handleReconnect() throws KattaException {
      LOG.info("Reconnecting load test node.");
      announceTestSearcher(_metaData);
    }
  }

  public LoadTestNode(final ZKClient zkClient, LoadTestNodeConfiguration configuration) throws KattaException {
    _zkClient = zkClient;
    _configuration = configuration;
    try {
      _statisticsWriter = new OutputStreamWriter(new FileOutputStream(_configuration.getStatisticsFile()), "UTF-8");
    } catch (IOException e) {
      throw new KattaException("Failed to create statistics output file.", e);
    }
  }

  public void start() throws KattaException {
    LOG.debug("Starting zk client...");
    _zkClient.getEventLock().lock();
    try {
      if (!_zkClient.isStarted()) {
        _zkClient.start(30000);
      }
      _zkClient.subscribeReconnects(new ReconnectListener());
    } finally {
      _zkClient.getEventLock().unlock();
    }
    startRpcServer(_configuration.getStartPort());
    _metaData = new TestSearcherMetaData();
    _metaData.setHost(getRpcHostName());
    _metaData.setPort(getRpcServerPort());
    announceTestSearcher(_metaData);
  }

  void announceTestSearcher(TestSearcherMetaData metaData) throws KattaException {
    LOG.info("Announcing new node.");
    if (_currentNodeName != null) {
      _zkClient.deleteIfExists(_currentNodeName);
    }
    _currentNodeName = _zkClient.create(ZkPathes.LOADTEST_NODES + "/node-", metaData, CreateMode.EPHEMERAL_SEQUENTIAL);
  }

  public void shutdown() {
    _shutdownLock.lock();
    try {
      if (_shutdown) {
        return;
      }
      _shutdown = true;
      stopRpcServer();
      _executorService.shutdown();
      try {
        _executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e1) {
        // ignore
      }

      LOG.info("Closing stream.");
      try {
        _statisticsWriter.close();
      } catch (IOException e) {
        LOG.warn("Failed to close statistics file.", e);
      }

      _zkClient.close();
    } finally {
      _shutdownLock.unlock();
    }
  }

  @Override
  public void startTest(int threads, final String[] indexNames, final String queryString, final int count) {
    LOG.info("Requested to run test with " + threads + " threads.");
    _executorService = Executors.newScheduledThreadPool(threads);
    _zkClient.unsubscribeAll();
    for (int i = 0; i < threads; i++) {
      _executorService.submit(new TestSearcherRunnable(count, indexNames, queryString));
    }
  }

  void writeStatistics(long elapsedTime) {
    try {
      _statisticsWriter.write("" + elapsedTime + "\n");
    } catch (IOException e) {
      LOG.warn("Could not write statistics data.", e);
    }
  }

  @Override
  public void stopTest() {
    LOG.info("Requested to stop test.");
    _executorService.shutdown();

    // shutdown from different thread
    new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          // ignore
        }
        shutdown();
      }
    }.start();
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  @Override
  protected void setup() {
    // do nothing
  }
}
