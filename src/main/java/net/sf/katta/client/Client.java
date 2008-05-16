/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.IPaths;
import net.sf.katta.slave.DocumentFrequenceWritable;
import net.sf.katta.slave.Hit;
import net.sf.katta.slave.Hits;
import net.sf.katta.slave.HitsMapWritable;
import net.sf.katta.slave.IQuery;
import net.sf.katta.slave.ISearch;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZKEventListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.lucene.analysis.Analyzer;

import com.yahoo.zookeeper.proto.WatcherEvent;

public class Client implements IClient {
  // TODO i see much space for improvement here, for example we do not need to
  // reload all index shards ...

  private final ZKClient _client;

  private final IndexDataListener _indexDataChangeListener = new IndexDataListener();

  private final IndexPathListener _indexPathChangeListener = new IndexPathListener();

  private final ShardListener _shardListener = new ShardListener();

  private final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();

  private final Map<String, List<String>> _shardsToSlave = new HashMap<String, List<String>>();

  private final Map<String, ISearch> _slaves = new HashMap<String, ISearch>();

  private final ISlaveSelectionPolicy _policy;

  private long _queryCount = 0;

  private final long _start;

  public Client(final ISlaveSelectionPolicy slaveSelectionPolicy) throws KattaException {
    this(slaveSelectionPolicy, new ZkConfiguration());
  }

  public Client() throws KattaException {
    this(new DefaultSlaveSelectionPolicy(), new ZkConfiguration());
  }

  public Client(final ISlaveSelectionPolicy policy, final ZkConfiguration config) throws KattaException {
    _policy = policy;
    _client = new ZKClient(config);
    synchronized (_client.getSyncMutex()) {
      _client.waitForZooKeeper(5000);
      // first get all changes on index..
      _client.subscribeChildChanges(IPaths.INDEXES, _indexPathChangeListener);
      loadIndexAndShardsData();
    }
    _start = System.currentTimeMillis();

  }

  private void loadIndexAndShardsData() throws KattaException {
    final List<String> knownIndexes = _client.getChildren(IPaths.INDEXES);
    for (final String indexName : knownIndexes) {
      loadShardsFromIndex(indexName);
    }
    // set datat for policy
    if (_indexToShards.size() != 0 && _shardsToSlave.size() != 0) {
      _policy.setShardsAndSlaves(_indexToShards, _shardsToSlave);
      // create slave connections..
      createSlaveConnections();
    }
  }

  private void createSlaveConnections() {
    final Collection<List<String>> values = _shardsToSlave.values();
    for (final List<String> slaveList : values) {
      for (final String slave : slaveList) {
        if (!_slaves.containsKey(slave)) {
          final ISearch slaveProxy = getSlaveProxy(slave);
          _slaves.put(slave, slaveProxy);
        }
      }
    }
  }

  private ISearch getSlaveProxy(final String slave) {
    ISearch slaveProxy = null;
    final Configuration configuration = new Configuration();
    final int splitPoint = slave.indexOf(':');
    if (-1 != splitPoint) {
      Logger.debug("connecting to slave: " + slave);
      final String serverName = slave.substring(0, splitPoint);
      final String port = slave.substring(splitPoint + 1, slave.length());
      try {
        slaveProxy = (ISearch) RPC.getProxy(ISearch.class, 0L,
            new InetSocketAddress(serverName, Integer.parseInt(port)), configuration);
      } catch (final IOException e) {
        Logger.warn("One of the slaves cannot be reached.", e);
      } catch (final NumberFormatException e) {
        Logger.warn("The supplied slave port is wrong '" + port + "'");
      }
    } else {
      Logger.warn("The format of the supplied slave address is wrong: '" + slave
          + "'. It should be a server name with a port number devided by a ':'.");
    }

    if (slaveProxy == null) {
      throw new RuntimeException("Unable to create slave proxy");
    }

    return slaveProxy;
  }

  private void loadShardsFromIndex(final String indexName) throws KattaException {
    final String indexPath = IPaths.INDEXES + "/" + indexName;
    final IndexMetaData indexMetaData = new IndexMetaData();
    _client.readData(indexPath, indexMetaData);
    if (indexMetaData.isDeployed()) {
      final List<String> indexShards = _client.getChildren(indexPath);
      _indexToShards.put(indexName, indexShards);
      for (final String shardName : indexShards) {
        final ArrayList<String> slaves = _client.subscribeChildChanges(IPaths.SHARD_TO_SLAVE + "/" + shardName,
            _shardListener);
        _shardsToSlave.put(shardName, slaves);
      }
    } else {
      _client.subscribeDataChanges(indexPath, _indexDataChangeListener);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#search(net.sf.katta.slave.IQuery,
   * java.lang.String[])
   */
  public Hits search(final IQuery query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#search(net.sf.katta.slave.IQuery,
   * java.lang.String[], int)
   */
  public Hits search(final IQuery query, final String[] indexNames, final int count) throws KattaException {
    final Map<String, List<String>> slaveShardsMap = _policy.getSlaveShardsMap(query, indexNames);
    Logger.info("Client.search()" + slaveShardsMap);
    final Hits result = new Hits();

    final DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();
    try {
      getDocFrequencies(query, slaveShardsMap, docFreqs);
    } catch (final IOException e) {
      throw new KattaException(e.getMessage(), e);
    }

    final List<Thread> searchThreads = new ArrayList<Thread>(slaveShardsMap.size());
    final Set<String> keySet = slaveShardsMap.keySet();
    for (final String slave : keySet) {
      final ISearch searchSlave = _slaves.get(slave);
      final List<String> shards = slaveShardsMap.get(slave);
      final Thread searchThread = new SearchThread(query, docFreqs, searchSlave, shards, result, slave, count);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    long start = 0;
    if (Logger.isDebug()) {
      start = System.currentTimeMillis();
    }
    threadJoin(searchThreads);
    if (Logger.isDebug()) {
      final long end = System.currentTimeMillis();
      Logger.debug("Time for searching: " + (end - start) / 1000.0);
    }

    if (Logger.isDebug()) {
      start = System.currentTimeMillis();
    }
    result.sort(count);
    if (Logger.isDebug()) {
      final long end = System.currentTimeMillis();
      Logger.debug("Time for sorting: " + (end - start) / 1000.0);
    }
    _queryCount++;
    return result;
  }

  private void threadJoin(final List<Thread> searchThreads) {
    for (final Thread thread : searchThreads) {
      try {
        thread.join();
      } catch (final InterruptedException e) {
        Logger.warn("Join for search thread interrupted.", e);
      }
    }
  }

  private void getDocFrequencies(final IQuery query, final Map<String, List<String>> slaveShardsMap,
      final DocumentFrequenceWritable docFreqs) throws IOException {
    final List<Thread> searchThreads = new ArrayList<Thread>(slaveShardsMap.size());
    final Set<String> keySet = slaveShardsMap.keySet();
    for (final String slave : keySet) {
      final ISearch searchSlave = _slaves.get(slave);
      final List<String> shards = slaveShardsMap.get(slave);
      final Thread documentFrequencyThread = new GetDocumentFrequencyThread(searchSlave, query, docFreqs, slave, shards);
      documentFrequencyThread.start();
      searchThreads.add(documentFrequencyThread);
    }

    // final long start = System.currentTimeMillis();
    threadJoin(searchThreads);
    // final long end = System.currentTimeMillis();
    // Logger.info("Time for getting document frequencies: " + (end - start)
    // / 1000.0);
  }

  private class IndexDataListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      // a existing index is now deployed..
      synchronized (_client.getSyncMutex()) {
        final String path = event.getPath();
        final IndexMetaData indexMetaData = new IndexMetaData();
        try {
          _client.readData(path, indexMetaData);
          if (indexMetaData.isDeployed()) {
            final String indexName = _client.getNodeNameFromPath(path);
            loadShardsFromIndex(indexName);
            // set datat for policy
            _policy.setShardsAndSlaves(_indexToShards, _shardsToSlave);
            // create slave connections..
            createSlaveConnections();
          }
        } catch (final KattaException e) {
          throw new RuntimeException("unable to read zookeeper data", e);
        }
      }
    }
  }

  private class IndexPathListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      // new index added...
      synchronized (_client.getSyncMutex()) {
        try {
          loadIndexAndShardsData();
          _policy.setShardsAndSlaves(_indexToShards, _shardsToSlave);
          // create slave connections..
          createSlaveConnections();
        } catch (final KattaException e) {
          Logger.error("Failed to read zookeeper information", e);
        }
        // set datat for policy
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#getDetails(net.sf.katta.slave.Hit)
   */
  public MapWritable getDetails(final Hit hit) throws IOException {
    final ISearch searchSlave = _slaves.get(hit.getSlave());
    // TODO only risk would be that between search and get detail the slave
    // crashs.
    return searchSlave.getDetails(hit.getShard(), hit.getDocId());
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#getDetails(net.sf.katta.slave.Hit,
   * java.lang.String)
   */
  public MapWritable getDetails(final Hit hit, final String[] fields) throws IOException {
    final ISearch searchSlave = _slaves.get(hit.getSlave());
    // TODO only risk would be that between search and get detail the slave
    // crashs.
    return searchSlave.getDetails(hit.getShard(), hit.getDocId(), fields);
  }

  private class ShardListener implements IZKEventListener {

    public void process(final WatcherEvent event) {
      // a shard got a new slave or one was removed...
      synchronized (_client.getSyncMutex()) {
        final String shardPath = event.getPath();
        List<String> newSlaves;
        try {
          newSlaves = _client.getChildren(shardPath);
          final String shardName = _client.getNodeNameFromPath(shardPath);
          final List<String> oldSlaves = _shardsToSlave.get(shardName);
          final List<String> toRemove = ComparisonUtil.getRemoved(oldSlaves, newSlaves);
          for (final String slave : toRemove) {
            oldSlaves.remove(slave);
            // TODO do we need to shut thoese down..?
            _slaves.remove(slave);
          }
          final List<String> toAdd = ComparisonUtil.getNew(oldSlaves, newSlaves);
          for (final String slave : toAdd) {
            oldSlaves.add(slave);
            _slaves.put(slave, getSlaveProxy(slave));
          }
        } catch (final KattaException e) {
          throw new RuntimeException("Failed to read zookeeper data.", e);
        }
      }
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#getQueryPerMinute()
   */
  public float getQueryPerMinute() {
    long time = (System.currentTimeMillis() - _start) / (60 * 1000);
    time = Math.max(time, 1);
    return (float) _queryCount / time;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#count(net.sf.katta.slave.IQuery,
   * java.lang.String[])
   */
  public int count(final IQuery query, final String[] indexNames) {
    final Map<String, List<String>> slaveShardsMap = _policy.getSlaveShardsMap(query, indexNames);
    Logger.info("Client.count()" + slaveShardsMap);
    final List<Integer> result = new ArrayList<Integer>();

    final long start = System.currentTimeMillis();
    final List<Thread> searchThreads = new ArrayList<Thread>();
    final Set<String> keySet = slaveShardsMap.keySet();
    for (final String slave : keySet) {
      final ISearch searchSlave = _slaves.get(slave);
      final List<String> shards = slaveShardsMap.get(slave);
      final Runnable searchRunnable = new ResultCountThread(query, searchSlave, shards, result, slave);
      final Thread searchThread = new Thread(searchRunnable, slave);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    threadJoin(searchThreads);
    final long end = System.currentTimeMillis();
    Logger.info("Time for counting: " + (end - start) / 1000.0);

    int resultCount = 0;
    for (final Integer count : result) {
      resultCount += count.intValue();
    }

    return resultCount;
  }

  public void addIndex(final String indexName, final String pathToIndex, final Analyzer analyzer) throws KattaException {
    _client.create(IPaths.INDEXES + "/" + indexName, new IndexMetaData(pathToIndex, analyzer.getClass().getName(),
        false));
  }

  // threads for searching..

  private class GetDocumentFrequencyThread extends Thread {

    private final ISearch _searchSlave;

    private final DocumentFrequenceWritable _docFreqs;

    private final String _slave;

    private final IQuery _query;

    private final List<String> _shards;

    public GetDocumentFrequencyThread(final ISearch searchSlave, final IQuery query,
        final DocumentFrequenceWritable docFreqs, final String slave, final List<String> shards) {
      _searchSlave = searchSlave;
      _query = query;
      _docFreqs = docFreqs;
      _slave = slave;
      _shards = shards;
    }

    @Override
    public void run() {
      try {
        long startThread = 0;
        if (Logger.isDebug()) {
          startThread = System.currentTimeMillis();
        }
        final DocumentFrequenceWritable slaveDocFreqs = _searchSlave.getDocFreqs(_query, _shards
            .toArray(new String[_shards.size()]));
        _docFreqs.addNumDocs(slaveDocFreqs.getNumDocs());
        _docFreqs.putAll(slaveDocFreqs.getAll());
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _slave + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
        }
      } catch (final IOException e) {
        // TODO we should Throw an Exception here since the results are
        // not correct..
        Logger.error("Cannot open searcher.", e);
      }
    }
  }

  private class SearchThread extends Thread {

    private final IQuery _query;

    private final ISearch _searchSlave;

    private final List<String> _shards;

    private final Hits _result;

    private final String _slave;

    private final int _count;

    private final DocumentFrequenceWritable _docFreqs;

    public SearchThread(final IQuery query, final DocumentFrequenceWritable docFreqs, final ISearch searchSlave,
        final List<String> shards, final Hits result, final String slave, final int count) {
      setName(slave);
      _query = query;
      _docFreqs = docFreqs;
      _searchSlave = searchSlave;
      _shards = shards;
      _result = result;
      _slave = slave;
      _count = count;
    }

    @Override
    public void run() {
      Hits hits = new Hits();
      try {
        long startThread = 0;
        if (Logger.isDebug()) {
          startThread = System.currentTimeMillis();
        }
        final String[] shardsArray = _shards.toArray(new String[_shards.size()]);
        final HitsMapWritable shardToHits = _searchSlave.search(_query, _docFreqs, shardsArray, _count);
        hits = shardToHits.getHits();
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _slave + " tooks " + (endThread - startThread) / 1000.0
              + "sec. Result size was " + hits.getHits().size());
        }
      } catch (final IOException e) {
        Logger.error("Cannot open searcher.", e);
      }
      _result.addHits(hits.getHits());
      _result.addTotalHits(hits.size());
    }

  }

  public class ResultCountThread extends Thread {

    private final String _slave;
    private final List<Integer> _result;
    private final ISearch _searchSlave;
    private final List<String> _shards;
    private final IQuery _query;

    public ResultCountThread(final IQuery query, final ISearch searchSlave, final List<String> shards,
        final List<Integer> result, final String slave) {
      _query = query;
      _searchSlave = searchSlave;
      _shards = shards;
      _result = result;
      _slave = slave;
    }

    @Override
    public void run() {
      try {
        long startThread = 0;
        if (Logger.isDebug()) {
          startThread = System.currentTimeMillis();
        }
        final int count = _searchSlave.getResultCount(_query, _shards.toArray(new String[_shards.size()]));
        _result.add(count);
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _slave + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
        }
      } catch (final IOException e) {
        Logger.error("Cannot open searcher.", e);
      }
    }

  }

}
