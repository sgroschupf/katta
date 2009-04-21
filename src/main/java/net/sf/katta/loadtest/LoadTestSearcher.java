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
import java.net.BindException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.Katta;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class LoadTestSearcher implements TestCommandListener {

  private final static Logger LOG = Logger.getLogger(LoadTestSearcher.class);

  private ZKClient _zkClient;
  private Server _rpcServer;
  ScheduledExecutorService _executorService = Executors.newScheduledThreadPool(1);
  private Writer _statisticsWriter;

  private Lock _shutdownLock = new ReentrantLock(true);
  private volatile boolean _shutdown = false;

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
      // TODO PVo configurable
      _executorService.schedule(this, _random.nextInt(500), TimeUnit.MILLISECONDS);
    }
  }

  public LoadTestSearcher(final ZKClient zkClient) throws KattaException {
    _zkClient = zkClient;
    try {
      // TODO PVo change file (configurable)
      _statisticsWriter = new OutputStreamWriter(new FileOutputStream("build/test-run.log"), "UTF-8");
    } catch (IOException e) {
      throw new KattaException("Failed to create statistics output file.", e);
    }
  }

  public void start() throws KattaException {
    _zkClient.getEventLock().lock();
    try {
      LOG.debug("Starting zk client...");
      if (!_zkClient.isStarted()) {
        _zkClient.start(30000);
      }
      TestSearcherMetaData metaData = startRpcServer();
      announceTestSearcher(metaData);
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  private void announceTestSearcher(TestSearcherMetaData metaData) throws KattaException {
    _zkClient.getEventLock().lock();
    try {
      _zkClient.create(ZkPathes.LOADTEST_NODES + "/node-", metaData, CreateMode.EPHEMERAL_SEQUENTIAL);
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  public void shutdown() {
    _shutdownLock.lock();
    try {
      if (_shutdown) {
        return;
      }
      _shutdown = true;
      _rpcServer.stop();
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

      _zkClient.getEventLock().lock();
      try {
        _zkClient.close();
      } finally {
        _zkClient.getEventLock().unlock();
      }
    } finally {
      _shutdownLock.unlock();
    }
  }

  /**
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of node.server.port.start + 10000
   */
  // TODO PVo duplicated code
  private TestSearcherMetaData startRpcServer() {
    // TODO PVo configurable??
    int startPort = 17676;
    int tryCount = 10000;

    TestSearcherMetaData metaData = new TestSearcherMetaData();
    metaData.setHost(NetworkUtil.getLocalhostName());
    int serverPort = startPort;
    while (_rpcServer == null) {
      try {
        _rpcServer = RPC.getServer(this, "0.0.0.0", serverPort, new Configuration());
        LOG.info("Search server started on : " + metaData.getHost() + ":" + serverPort);
        metaData.setPort(serverPort);
      } catch (final BindException e) {
        if (startPort - serverPort < tryCount) {
          serverPort++;
          // try again
        } else {
          throw new RuntimeException("Tried " + tryCount + " ports and no one is free...");
        }
      } catch (final IOException e) {
        throw new RuntimeException("Unable to create rpc search server", e);
      }
    }

    try {
      _rpcServer.start();
    } catch (final IOException e) {
      throw new RuntimeException("Failed to start rpc search server", e);
    }
    return metaData;
  }

  public void join() throws InterruptedException {
    _rpcServer.join();
  }

  @Override
  public void startTest(int threads, final String[] indexNames, final String queryString, final int count) {
    LOG.info("Requested to run test with " + threads + " threads.");
    _executorService = Executors.newScheduledThreadPool(threads);
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
}
