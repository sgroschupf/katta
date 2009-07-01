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


public class LoadTestStarter {

//  static final Logger LOG = Logger.getLogger(LoadTestStarter.class);
//
//  ZKClient _zkClient;
//  private Map<String, LoadTestNodeMetaData> _testNodes = new HashMap<String, LoadTestNodeMetaData>();
//  private int _numberOfTesterNodes;
//  private int _startRate;
//  private int _endRate;
//  private int _step;
//
//  ChildListener _childListener;
//
//  private String[] _indexNames;
//  private String _queryFile;
//  private int _count;
//  private int _runTime;
//  private Writer _statisticsWriter;
//  private Writer _resultWriter;
//
//  class ChildListener implements IZkChildListener {
//    @Override
//    public void handleChildChange(String parentPath, List<String> currentChilds) throws KattaException {
//      checkNodes(currentChilds);
//    }
//  }
//
//  class ReconnectListener implements IZkReconnectListener {
//
//    @Override
//    public void handleNewSession() throws Exception {
//      // do nothing
//    }
//
//    @Override
//    public void handleStateChanged(KeeperState state) throws Exception {
//      if (state == KeeperState.SyncConnected) {
//        LOG.info("Reconnecting test starter.");
//        checkNodes(_zkClient.getChildren(ZkPathes.LOADTEST_NODES));
//      }
//    }
//  }
//
//  public LoadTestStarter(final ZKClient zkClient, int nodes, int startRate, int endRate, int step, int runTime,
//          String[] indexNames, String queryFile, int count) throws KattaException {
//    _zkClient = zkClient;
//    _numberOfTesterNodes = nodes;
//    _startRate = startRate;
//    _endRate = endRate;
//    _step = step;
//    _indexNames = indexNames;
//    _queryFile = queryFile;
//    _count = count;
//    _runTime = runTime;
//    try {
//      long currentTime = System.currentTimeMillis();
//      _statisticsWriter = new OutputStreamWriter(new FileOutputStream("build/load-test-log-" + currentTime + ".log"));
//      _resultWriter = new OutputStreamWriter(new FileOutputStream("build/load-test-results-" + currentTime + ".log"));
//    } catch (FileNotFoundException e) {
//      throw new KattaException("Failed to create statistics file.", e);
//    }
//  }
//
//  public void start() throws KattaException {
//    LOG.debug("Starting zk client...");
//    _zkClient.getEventLock().lock();
//    try {
//      if (!_zkClient.isStarted()) {
//        _zkClient.start(30000);
//      }
//      _zkClient.subscribeReconnects(new ReconnectListener());
//      _childListener = new ChildListener();
//      _zkClient.subscribeChildChanges(ZkPathes.LOADTEST_NODES, _childListener);
//      checkNodes(_zkClient.getChildren(ZkPathes.LOADTEST_NODES));
//    } finally {
//      _zkClient.getEventLock().unlock();
//    }
//  }
//
//  void checkNodes(List<String> children) throws KattaException {
//    synchronized (_testNodes) {
//      LOG.info("Nodes found: " + children);
//
//      Set<String> obsoleteNodes = new HashSet<String>(_testNodes.keySet());
//      obsoleteNodes.removeAll(children);
//      for (String obsoleteNode : obsoleteNodes) {
//        LOG.info("Lost connection to " + obsoleteNode);
//        _testNodes.remove(obsoleteNode);
//      }
//
//      for (String child : children) {
//        if (!_testNodes.containsKey(child)) {
//          try {
//            LoadTestNodeMetaData metaData = new LoadTestNodeMetaData();
//            _zkClient.readData(ZkPathes.LOADTEST_NODES + "/" + child, metaData);
//            LOG.info("New test node on " + metaData.getHost() + ":" + metaData.getPort());
//            _testNodes.put(child, metaData);
//          } catch (KattaException e) {
//            LOG.info("Could not read meta data of load test node: " + child + ". It probably disappeared.");
//          }
//        }
//      }
//      if (_testNodes.size() >= _numberOfTesterNodes) {
//        startTest();
//      }
//    }
//  }
//
//  private void startTest() throws KattaException {
//    List<LoadTestNodeMetaData> testers = new ArrayList<LoadTestNodeMetaData>(_testNodes.values());
//    List<ILoadTestNode> testNodes = new ArrayList<ILoadTestNode>();
//    for (int i = 0; i < _numberOfTesterNodes; i++) {
//      try {
//        testNodes.add((ILoadTestNode) RPC.getProxy(ILoadTestNode.class, 0, new InetSocketAddress(testers.get(i)
//                .getHost(), testers.get(i).getPort()), new Configuration()));
//      } catch (IOException e) {
//        throw new KattaException("Failed to start tests.", e);
//      }
//    }
//    _zkClient.unsubscribeAll();
//    for (int queryRate = _startRate; queryRate <= _endRate; queryRate += _step) {
//      LOG.info("Executing tests at query rate: " + queryRate + " queries per second.");
//      int numberOfNodes = Math.min(testNodes.size(), queryRate);
//      LOG.info("Using " + numberOfNodes + " load test nodes for this test.");
//      List<ILoadTestNode> nodesForTest = testNodes.subList(0, numberOfNodes);
//
//      int remainingQueryRate = queryRate;
//      int remainingNodes = numberOfNodes;
//      String[] queries;
//      try {
//        queries = readQueries(new FileInputStream(_queryFile));
//      } catch (IOException e) {
//        throw new KattaException("Failed to read query file " + _queryFile + ".", e);
//      }
//      for (ILoadTestNode testNode : nodesForTest) {
//        int queryRateForNode = remainingQueryRate / remainingNodes;
//        LOG.info("Initializing test on node using query rate: " + queryRateForNode + " queries per second.");
//        testNode.initTest(queryRateForNode, _indexNames, queries, _count);
//        --remainingNodes;
//        remainingQueryRate -= queryRateForNode;
//      }
//      for (ILoadTestNode testNode : nodesForTest) {
//        LOG.info("Starting test on node.");
//        testNode.startTest();
//      }
//      try {
//        Thread.sleep(_runTime);
//      } catch (InterruptedException e) {
//        // ignore
//      }
//      LOG.info("Stopping all tests...");
//      for (ILoadTestNode testNode : nodesForTest) {
//        testNode.stopTest();
//      }
//      LOG.info("Collecting results...");
//      List<LoadTestQueryResult> results = new ArrayList<LoadTestQueryResult>();
//      for (ILoadTestNode testNode : nodesForTest) {
//        LoadTestQueryResult[] nodeResults = testNode.getResults();
//        for (LoadTestQueryResult result : nodeResults) {
//          results.add(result);
//        }
//      }
//      LOG.info("Received " + results.size() + " queries, expected " + queryRate * _runTime / 1000);
//      try {
//        StorelessUnivariateStatistic timeStandardDeviation = new StandardDeviation();
//        StorelessUnivariateStatistic timeMean = new Mean();
//        int errors = 0;
//
//        for (LoadTestQueryResult result : results) {
//          long elapsedTime = result.getEndTime() > 0 ? result.getEndTime() - result.getStartTime() : -1;
//          _statisticsWriter.write(queryRate + "\t" + result.getNodeId() + "\t" + result.getStartTime() + "\t"
//                  + result.getEndTime() + "\t" + elapsedTime + "\t" + result.getQuery() + "\n");
//          if (elapsedTime != -1) {
//            timeStandardDeviation.increment(elapsedTime);
//            timeMean.increment(elapsedTime);
//          } else {
//            ++errors;
//          }
//        }
//        _resultWriter.write(queryRate + "\t" + ((double) results.size() * 1000 / _runTime) + "\t" + errors + "\t"
//                + timeMean.getResult() + "\t" + timeStandardDeviation.getResult() + "\n");
//      } catch (IOException e) {
//        throw new KattaException("Failed to write statistics data.", e);
//      }
//    }
//    try {
//      _statisticsWriter.close();
//      _resultWriter.close();
//    } catch (IOException e) {
//      LOG.warn("Failed to close statistics file.");
//    }
//    shutdown();
//  }
//
//  static String[] readQueries(InputStream inputStream) throws IOException {
//    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//    List<String> lines = new ArrayList<String>();
//    String line;
//    while ((line = reader.readLine()) != null) {
//      line = line.trim();
//      if (!line.equals("")) {
//        lines.add(line);
//      }
//    }
//    return lines.toArray(new String[lines.size()]);
//  }
//
//  public void shutdown() {
//    _zkClient.getEventLock().lock();
//    try {
//      _zkClient.unsubscribeAll();
//      _zkClient.close();
//    } finally {
//      _zkClient.getEventLock().unlock();
//    }
//  }
}
