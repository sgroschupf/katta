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
import net.sf.katta.node.DocumentFrequenceWritable;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.ISearch;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZKEventListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.RPC;

import com.yahoo.zookeeper.proto.WatcherEvent;

/**
 * Default implementation of {@link IClient}.
 * 
 */
public class Client implements IClient {
  // TODO i see much space for improvement here, for example we do not need to
  // reload all index shards ...

  private final ZKClient _client;

  private final IndexDataListener _indexDataChangeListener = new IndexDataListener();

  private final IndexPathListener _indexPathChangeListener = new IndexPathListener();

  private final ShardListener _shardListener = new ShardListener();

  private final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();

  private final Map<String, List<String>> _shardsToNode = new HashMap<String, List<String>>();

  private final Map<String, ISearch> _nodes = new HashMap<String, ISearch>();

  private final INodeSelectionPolicy _policy;

  private long _queryCount = 0;

  private final long _start;

  public Client(final INodeSelectionPolicy nodeSelectionPolicy) throws KattaException {
    this(nodeSelectionPolicy, new ZkConfiguration());
  }

  public Client() throws KattaException {
    this(new DefaultNodeSelectionPolicy(), new ZkConfiguration());
  }

  public Client(final INodeSelectionPolicy policy, final ZkConfiguration config) throws KattaException {
    _policy = policy;
    _client = new ZKClient(config);
    synchronized (_client.getSyncMutex()) {
      _client.waitForZooKeeper(30000);
      // first get all changes on index..
      _client.createDefaultNameSpace();
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
    if (_indexToShards.size() != 0 && _shardsToNode.size() != 0) {
      _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
      // create node connections..
      createNodeConnections();
    }
  }

  private void createNodeConnections() {
    final Collection<List<String>> values = _shardsToNode.values();
    for (final List<String> nodeList : values) {
      for (final String node : nodeList) {
        if (!_nodes.containsKey(node)) {
          final ISearch nodeProxy = getNodeProxy(node);
          _nodes.put(node, nodeProxy);
        }
      }
    }
  }

  private ISearch getNodeProxy(final String node) {
    ISearch nodeProxy = null;
    final Configuration configuration = new Configuration();
    final int splitPoint = node.indexOf(':');
    if (-1 != splitPoint) {
      Logger.debug("connecting to node: " + node);
      final String serverName = node.substring(0, splitPoint);
      final String port = node.substring(splitPoint + 1, node.length());
      try {
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(serverName, Integer.parseInt(port));
        nodeProxy = (ISearch) RPC.getProxy(ISearch.class, 0L, inetSocketAddress, configuration);
      } catch (final IOException e) {
        Logger.warn("One of the nodes cannot be reached.", e);
      } catch (final NumberFormatException e) {
        Logger.warn("The supplied node port is wrong '" + port + "'");
      }
    } else {
      Logger.warn("The format of the supplied node address is wrong: '" + node
          + "'. It should be a server name with a port number devided by a ':'.");
    }

    if (nodeProxy == null) {
      throw new RuntimeException("Unable to create node proxy");
    }

    return nodeProxy;
  }

  private void loadShardsFromIndex(final String indexName) throws KattaException {
    final String indexPath = IPaths.INDEXES + "/" + indexName;
    final IndexMetaData indexMetaData = new IndexMetaData();
    _client.readData(indexPath, indexMetaData);
    if (indexMetaData.getState() == IndexMetaData.IndexState.DEPLOYED) {
      final List<String> indexShards = _client.getChildren(indexPath);
      _indexToShards.put(indexName, indexShards);
      for (final String shardName : indexShards) {
        final ArrayList<String> nodes = _client.subscribeChildChanges(IPaths.SHARD_TO_NODE + "/" + shardName,
            _shardListener);
        Logger.debug("Add shard listener in client.");
        _shardsToNode.put(shardName, nodes);
      }
    } else {
      _client.subscribeDataChanges(indexPath, _indexDataChangeListener);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#search(net.sf.katta.node.IQuery,
   * java.lang.String[])
   */
  public Hits search(final IQuery query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#search(net.sf.katta.node.IQuery,
   * java.lang.String[], int)
   */
  public Hits search(final IQuery query, final String[] indexNames, final int count) throws KattaException {
    String[] indexesToSearchIn = indexNames;
    for (String indexName : indexNames) {
      if ("*".equals(indexName)) {
        Set<String> keySet = _indexToShards.keySet();
        indexesToSearchIn = new String[keySet.size()];
        indexesToSearchIn = keySet.toArray(indexesToSearchIn);
        break;
      }
    }
    final Map<String, List<String>> nodeShardsMap = _policy.getNodeShardsMap(query, indexesToSearchIn);
    Logger.info("Client.search()" + nodeShardsMap);
    final Hits result = new Hits();

    final DocumentFrequenceWritable docFreqs = getDocFrequencies(query, nodeShardsMap);
    final List<Thread> searchThreads = new ArrayList<Thread>(nodeShardsMap.size());
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodes.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Thread searchThread = new SearchThread(query, docFreqs, searchNode, shards, result, node, count);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    long start = 0;
    if (Logger.isDebug()) {
      start = System.currentTimeMillis();
    }
    joinThreads(searchThreads);
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

  private void joinThreads(final List<Thread> searchThreads) {
    try {
      for (final Thread thread : searchThreads) {
        thread.join();
      }
    } catch (final InterruptedException e) {
      Logger.warn("Join for search threads interrupted.", e);
    }
  }

  private DocumentFrequenceWritable getDocFrequencies(final IQuery query, final Map<String, List<String>> nodeShardsMap) {
    DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();
    final List<Thread> searchThreads = new ArrayList<Thread>(nodeShardsMap.size());
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodes.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Thread documentFrequencyThread = new GetDocumentFrequencyThread(searchNode, query, docFreqs, node, shards);
      documentFrequencyThread.start();
      searchThreads.add(documentFrequencyThread);
    }

    // final long start = System.currentTimeMillis();
    joinThreads(searchThreads);
    // final long end = System.currentTimeMillis();
    // Logger.info("Time for getting document frequencies: " + (end - start)
    // / 1000.0);
    return docFreqs;
  }

  private class IndexDataListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      // a existing index is now deployed..
      synchronized (_client.getSyncMutex()) {
        final String path = event.getPath();
        final IndexMetaData indexMetaData = new IndexMetaData();
        try {
          _client.readData(path, indexMetaData);
          if (indexMetaData.getState() == IndexMetaData.IndexState.DEPLOYED) {
            final String indexName = _client.getNodeNameFromPath(path);
            loadShardsFromIndex(indexName);
            // set datat for policy
            _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
            // create node connections..
            createNodeConnections();
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
          _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
          // create node connections..
          createNodeConnections();
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
   * @see net.sf.katta.client.IClient#getDetails(net.sf.katta.node.Hit)
   */
  public MapWritable getDetails(final Hit hit) throws IOException {
    return getDetails(hit, null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.client.IClient#getDetails(net.sf.katta.node.Hit,
   * java.lang.String)
   */
  public MapWritable getDetails(final Hit hit, final String[] fields) throws IOException {
    final ISearch searchNode = _nodes.get(hit.getNode());
    // TODO only risk would be that between search and get detail the node
    // crashs.
    MapWritable details;
    if (fields == null) {
      details = searchNode.getDetails(hit.getShard(), hit.getDocId());
    } else {
      details = searchNode.getDetails(hit.getShard(), hit.getDocId(), fields);
    }

    return details;
  }

  private class ShardListener implements IZKEventListener {

    public void process(final WatcherEvent event) {
      Logger.debug("Shard event in client.");
      // a shard got a new node or one was removed...
      synchronized (_client.getSyncMutex()) {
        final String shardPath = event.getPath();
        List<String> newNodes;
        try {
          newNodes = _client.getChildren(shardPath);
          final String shardName = _client.getNodeNameFromPath(shardPath);
          final List<String> oldNodes = _shardsToNode.get(shardName);
          final List<String> toRemove = ComparisonUtil.getRemoved(oldNodes, newNodes);
          for (final String node : toRemove) {
            oldNodes.remove(node);
            // TODO do we need to shut thoese down..?
            _nodes.remove(node);
          }
          final List<String> toAdd = ComparisonUtil.getNew(oldNodes, newNodes);
          for (final String node : toAdd) {
            oldNodes.add(node);
            _nodes.put(node, getNodeProxy(node));
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
   * @see net.sf.katta.client.IClient#count(net.sf.katta.node.IQuery,
   * java.lang.String[])
   */
  public int count(final IQuery query, final String[] indexNames) {
    String[] indexesToSearchIn = indexNames;
    for (String indexName : indexNames) {
      if ("*".equals(indexName)) {
        Set<String> keySet = _indexToShards.keySet();
        indexesToSearchIn = new String[keySet.size()];
        indexesToSearchIn = keySet.toArray(indexesToSearchIn);
        break;
      }
    }
    final Map<String, List<String>> nodeShardsMap = _policy.getNodeShardsMap(query, indexesToSearchIn);
    Logger.info("Client.count()" + nodeShardsMap);
    final List<Integer> result = new ArrayList<Integer>();

    final long start = System.currentTimeMillis();
    final List<Thread> searchThreads = new ArrayList<Thread>();
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodes.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Runnable searchRunnable = new ResultCountThread(query, searchNode, shards, result, node);
      final Thread searchThread = new Thread(searchRunnable, node);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    joinThreads(searchThreads);
    final long end = System.currentTimeMillis();
    Logger.info("Time for counting: " + (end - start) / 1000.0);

    int resultCount = 0;
    for (final Integer count : result) {
      resultCount += count.intValue();
    }

    return resultCount;
  }

  public void close() {
    if (_client != null) {
      _client.close();
    }
  }

  // threads for searching..

  private class GetDocumentFrequencyThread extends Thread {

    private final ISearch _searchNode;

    private final DocumentFrequenceWritable _docFreqs;

    private final String _node;

    private final IQuery _query;

    private final List<String> _shards;

    public GetDocumentFrequencyThread(final ISearch searchNode, final IQuery query,
        final DocumentFrequenceWritable docFreqs, final String node, final List<String> shards) {
      _searchNode = searchNode;
      _query = query;
      _docFreqs = docFreqs;
      _node = node;
      _shards = shards;
    }

    @Override
    public void run() {
      try {
        long startThread = 0;
        if (Logger.isDebug()) {
          startThread = System.currentTimeMillis();
        }
        final DocumentFrequenceWritable nodeDocFreqs = _searchNode.getDocFreqs(_query, _shards
            .toArray(new String[_shards.size()]));
        _docFreqs.addNumDocs(nodeDocFreqs.getNumDocs());
        _docFreqs.putAll(nodeDocFreqs.getAll());
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
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

    private final ISearch _searchNode;

    private final List<String> _shards;

    private final Hits _result;

    private final String _node;

    private final int _count;

    private final DocumentFrequenceWritable _docFreqs;

    public SearchThread(final IQuery query, final DocumentFrequenceWritable docFreqs, final ISearch searchNode,
        final List<String> shards, final Hits result, final String node, final int count) {
      setName(node);
      _query = query;
      _docFreqs = docFreqs;
      _searchNode = searchNode;
      _shards = shards;
      _result = result;
      _node = node;
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
        final HitsMapWritable shardToHits = _searchNode.search(_query, _docFreqs, shardsArray, _count);
        hits = shardToHits.getHits();
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0
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

    private final String _node;
    private final List<Integer> _result;
    private final ISearch _searchNode;
    private final List<String> _shards;
    private final IQuery _query;

    public ResultCountThread(final IQuery query, final ISearch searchNode, final List<String> shards,
        final List<Integer> result, final String node) {
      _query = query;
      _searchNode = searchNode;
      _shards = shards;
      _result = result;
      _node = node;
    }

    @Override
    public void run() {
      try {
        long startThread = 0;
        if (Logger.isDebug()) {
          startThread = System.currentTimeMillis();
        }
        final int count = _searchNode.getResultCount(_query, _shards.toArray(new String[_shards.size()]));
        _result.add(count);
        if (Logger.isDebug()) {
          final long endThread = System.currentTimeMillis();
          Logger.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
        }
      } catch (final IOException e) {
        Logger.error("Cannot open searcher, remove " + _node + " from connections.", e);
        _nodes.remove(_node);
      }
    }

  }

}
