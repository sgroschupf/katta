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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.util.KattaException;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class LoadTestStarter {

  static final Logger LOG = Logger.getLogger(LoadTestStarter.class);

  ZKClient _zkClient;
  private Map<String, LoadTestNodeMetaData> _testNodes = new HashMap<String, LoadTestNodeMetaData>();
  private int _numberOfTesterNodes;
  private int _fromThreads;
  private int _toThreads;

  ChildListener _childListener;

  private String[] _indexNames;
  private String _queryString;
  private int _count;
  private int _runTime;
  private Writer _statisticsWriter;

  class ChildListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws KattaException {
      checkNodes(currentChilds);
    }
  }

  class ReconnectListener implements IZkReconnectListener {

    @Override
    public void handleNewSession() throws Exception {
      // do nothing
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
      if (state == KeeperState.SyncConnected) {
        LOG.info("Reconnecting test starter.");
        checkNodes(_zkClient.getChildren(ZkPathes.LOADTEST_NODES));
      }
    }
  }

  public LoadTestStarter(final ZKClient zkClient, int nodes, int fromThreads, int toThreads, int runTime,
          String[] indexNames, String queryString, int count) throws KattaException {
    _zkClient = zkClient;
    _numberOfTesterNodes = nodes;
    _fromThreads = fromThreads;
    _toThreads = toThreads;
    _indexNames = indexNames;
    _queryString = queryString;
    _count = count;
    _runTime = runTime;
    try {
      _statisticsWriter = new OutputStreamWriter(new FileOutputStream("build/statistics-" + System.currentTimeMillis()
              + ".log"));
    } catch (FileNotFoundException e) {
      throw new KattaException("Failed to create statistics file.", e);
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
      _childListener = new ChildListener();
      _zkClient.subscribeChildChanges(ZkPathes.LOADTEST_NODES, _childListener);
      checkNodes(_zkClient.getChildren(ZkPathes.LOADTEST_NODES));
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  void checkNodes(List<String> children) throws KattaException {
    synchronized (_testNodes) {
      LOG.info("Nodes found: " + children);

      Set<String> obsoleteNodes = new HashSet<String>(_testNodes.keySet());
      obsoleteNodes.removeAll(children);
      for (String obsoleteNode : obsoleteNodes) {
        LOG.info("Lost connection to " + obsoleteNode);
        _testNodes.remove(obsoleteNode);
      }

      for (String child : children) {
        if (!_testNodes.containsKey(child)) {
          try {
            LoadTestNodeMetaData metaData = new LoadTestNodeMetaData();
            _zkClient.readData(ZkPathes.LOADTEST_NODES + "/" + child, metaData);
            LOG.info("New test node on " + metaData.getHost() + ":" + metaData.getPort());
            _testNodes.put(child, metaData);
          } catch (KattaException e) {
            LOG.info("Could not read meta data of load test node: " + child + ". It probably disappeared.");
          }
        }
      }
      if (_testNodes.size() >= _numberOfTesterNodes) {
        startTest();
      }
    }
  }

  private void startTest() throws KattaException {
    List<LoadTestNodeMetaData> testers = new ArrayList<LoadTestNodeMetaData>(_testNodes.values());
    List<ILoadTestNode> testNodes = new ArrayList<ILoadTestNode>();
    for (int i = 0; i < _numberOfTesterNodes; i++) {
      try {
        testNodes.add((ILoadTestNode) RPC.getProxy(ILoadTestNode.class, 0, new InetSocketAddress(testers.get(i)
                .getHost(), testers.get(i).getPort()), new Configuration()));
      } catch (IOException e) {
        throw new KattaException("Failed to start tests.", e);
      }
    }
    _zkClient.unsubscribeAll();
    for (int threads = _fromThreads; threads <= _toThreads; threads++) {
      for (ILoadTestNode testNode : testNodes) {
        LOG.info("Starting test on node.");
        testNode.startTest(threads, _indexNames, _queryString, _count);
      }
      try {
        Thread.sleep(_runTime);
      } catch (InterruptedException e) {
        // ignore
      }
      LOG.info("Stopping all tests...");
      for (ILoadTestNode testNode : testNodes) {
        testNode.stopTest();
      }
      LOG.info("Collecting results...");
      List<Integer> results = new ArrayList<Integer>();
      for (ILoadTestNode testNode : testNodes) {
        int[] nodeResults = testNode.getResults();
        for (int result : nodeResults) {
          results.add(result);
        }
      }
      try {
        for (Integer result : results) {
          _statisticsWriter.write(threads + "\t" + result + "\n");
        }
      } catch (IOException e) {
        throw new KattaException("Failed to write statistics data.", e);
      }
    }
    try {
      _statisticsWriter.close();
    } catch (IOException e) {
      LOG.warn("Failed to close statistics file.");
    }
    shutdown();
  }

  public void shutdown() {
    _zkClient.getEventLock().lock();
    try {
      _zkClient.unsubscribeAll();
      _zkClient.close();
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }
}
