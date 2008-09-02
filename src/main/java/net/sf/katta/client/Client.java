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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.node.DocumentFrequenceWritable;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.HitsMapWritable;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.ISearch;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkDataListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

/**
 * Default implementation of {@link IClient}.
 * 
 */
public class Client implements IClient {

  protected final static Logger LOG = Logger.getLogger(Client.class);
  // TODO i see much space for improvement here, for example we do not need to
  // reload all index shards ...

  protected final ZKClient _zkClient;

  private final IndexDataListener _indexDataChangeListener = new IndexDataListener();
  private final IndexPathListener _indexPathChangeListener = new IndexPathListener();
  private final ShardListener _shardListener = new ShardListener();

  protected final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();
  protected final Map<String, List<String>> _shardsToNode = new HashMap<String, List<String>>();
  protected final Map<String, ISearch> _nodeNames2Proxy = new HashMap<String, ISearch>();

  protected final INodeSelectionPolicy _policy;
  private long _queryCount = 0;
  private final long _start;

  private Configuration _hadoopConf = new Configuration();

  public Client(final INodeSelectionPolicy nodeSelectionPolicy) throws KattaException {
    this(nodeSelectionPolicy, new ZkConfiguration());
  }

  public Client() throws KattaException {
    this(new DefaultNodeSelectionPolicy(), new ZkConfiguration());
  }

  public Client(final INodeSelectionPolicy policy, final ZkConfiguration config) throws KattaException {
    _policy = policy;
    _zkClient = new ZKClient(config);
    try {
      _zkClient.getEventLock().lock();
      _zkClient.start(30000);
      // first get all changes on index..
      List<String> indexes = _zkClient.subscribeChildChanges(ZkPathes.INDEXES, _indexPathChangeListener);
      loadIndexAndShardsData(indexes);
    } finally {
      _zkClient.getEventLock().unlock();
    }
    _start = System.currentTimeMillis();
  }

  protected void loadIndexAndShardsData(List<String> indexes) throws KattaException {
    for (final String indexName : indexes) {
      loadShardsFromIndex(indexName);
    }
    // set data for policy
    if (_indexToShards.size() != 0 && _shardsToNode.size() != 0) {
      _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
      // create node connections..
      createNodeConnections();
    }
  }

  protected void createNodeConnections() {
    final Collection<List<String>> listOfNodeLists = _shardsToNode.values();
    Set<String> failedNodes = new HashSet<String>();
    for (final List<String> nodeList : listOfNodeLists) {
      for (final String node : nodeList) {
        try {
          if (!_nodeNames2Proxy.containsKey(node) && !failedNodes.contains(node)) {
            final ISearch nodeProxy = getNodeProxy(node);
            _nodeNames2Proxy.put(node, nodeProxy);
          }
        } catch (Exception e) {
          LOG.error("could not create proxy for node " + node);
          failedNodes.add(node);
        }
      }
    }

    // cleanup "unreachable" nodes
    for (final List<String> nodeList : listOfNodeLists) {
      for (Iterator iterator = nodeList.iterator(); iterator.hasNext();) {
        String node = (String) iterator.next();
        if (failedNodes.contains(node)) {
          iterator.remove();
        }
      }
    }
    if (!failedNodes.isEmpty()) {
      _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
    }
  }

  protected ISearch getNodeProxy(final String node) throws IOException {
    LOG.debug("creating proxy for node: " + node);

    String[] hostName_port = node.split(":");
    if (hostName_port.length != 2) {
      throw new RuntimeException("invalid node name format '" + node
          + "' (It should be a host name with a port number devided by a ':')");
    }
    final String hostName = hostName_port[0];
    final String port = hostName_port[1];
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, Integer.parseInt(port));
    return (ISearch) RPC.getProxy(ISearch.class, 0L, inetSocketAddress, _hadoopConf);
  }

  protected void loadShardsFromIndex(final String indexName) throws KattaException {
    final String indexPath = ZkPathes.getIndexPath(indexName);
    final IndexMetaData indexMetaData = new IndexMetaData();
    _zkClient.readData(indexPath, indexMetaData);
    if (indexMetaData.getState() == IndexMetaData.IndexState.DEPLOYED) {
      final List<String> indexShards = _zkClient.getChildren(indexPath);
      _indexToShards.put(indexName, indexShards);
      for (final String shardName : indexShards) {
        List<String> nodes = _zkClient.subscribeChildChanges(ZkPathes.getShard2NodeRootPath(shardName), _shardListener);
        _shardsToNode.put(shardName, nodes);
      }
    } else {
      _zkClient.subscribeDataChanges(indexPath, _indexDataChangeListener);
    }
  }

  public Hits search(final IQuery query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

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
    final Hits result = new Hits();

    final DocumentFrequenceWritable docFreqs = getDocFrequencies(query, nodeShardsMap);
    final List<Thread> searchThreads = new ArrayList<Thread>(nodeShardsMap.size());
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodeNames2Proxy.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Thread searchThread = new SearchThread(query, docFreqs, searchNode, shards, result, node, count);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    joinThreads(searchThreads);
    if (LOG.isDebugEnabled()) {
      final long end = System.currentTimeMillis();
      LOG.debug("Time for searching: " + (end - start) / 1000.0);
    }

    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    result.sort(count);
    if (LOG.isDebugEnabled()) {
      final long end = System.currentTimeMillis();
      LOG.debug("Time for sorting: " + (end - start) / 1000.0);
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
      LOG.warn("Join for search threads interrupted.", e);
    }
  }

  private DocumentFrequenceWritable getDocFrequencies(final IQuery query, final Map<String, List<String>> nodeShardsMap) {
    DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();
    final List<Thread> searchThreads = new ArrayList<Thread>(nodeShardsMap.size());
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodeNames2Proxy.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Thread documentFrequencyThread = new GetDocumentFrequencyThread(searchNode, query, docFreqs, node, shards);
      documentFrequencyThread.start();
      searchThreads.add(documentFrequencyThread);
    }

    // final long start = System.currentTimeMillis();
    joinThreads(searchThreads);
    // final long end = System.currentTimeMillis();
    // LOG.info("Time for getting document frequencies: " + (end - start)
    // / 1000.0);
    return docFreqs;
  }

  protected class IndexDataListener implements IZkDataListener<IndexMetaData> {

    public void handleDataAdded(String dataPath, IndexMetaData data) throws KattaException {
      // handled through IndexPathListener
    }

    public void handleDataChange(String dataPath, IndexMetaData data) throws KattaException {
      final String indexName = ZkPathes.getName(dataPath);
      loadShardsFromIndex(indexName);
      // set datat for policy
      _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
      // create node connections..
      createNodeConnections();
    }

    public void handleDataDeleted(String dataPath) throws KattaException {
      // handled through IndexPathListener
    }

    public IndexMetaData createWritable() {
      return new IndexMetaData();
    }

  }

  protected class IndexPathListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentIndexes) throws KattaException {
      // index added/removed...
      // TODO jz: i think index removal is not handled
      try {
        loadIndexAndShardsData(currentIndexes);
        _policy.setShardsAndNodes(_indexToShards, _shardsToNode);
        // create node connections..
        createNodeConnections();
      } catch (final KattaException e) {
        LOG.error("Failed to read zookeeper information", e);
      }
    }
  }

  public MapWritable getDetails(final Hit hit) throws IOException {
    return getDetails(hit, null);
  }

  public MapWritable getDetails(final Hit hit, final String[] fields) throws IOException {
    final ISearch searchNode = _nodeNames2Proxy.get(hit.getNode());
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

  protected class ShardListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentNodes) throws KattaException {
      LOG.debug("Shard event in client.");
      // a shard got a new node or one was removed...
      final String shardName = ZkPathes.getName(parentPath);
      final List<String> knownShardNodes = _shardsToNode.get(shardName);
      final List<String> toRemove = CollectionUtil.getListOfRemoved(knownShardNodes, currentNodes);
      for (final String node : toRemove) {
        knownShardNodes.remove(node);
        // TODO do we need to shut thoese down..? (hadoop0.17 has
        // RPC.stopProxy())
        _nodeNames2Proxy.remove(node);
      }
      final List<String> toAdd = CollectionUtil.getListOfAdded(knownShardNodes, currentNodes);
      for (final String node : toAdd) {
        knownShardNodes.add(node);
        try {
          _nodeNames2Proxy.put(node, getNodeProxy(node));
        } catch (Exception e) {
          LOG.error("could not create proxy for node " + node, e);
        }
      }
    }
  }

  public float getQueryPerMinute() {
    long time = (System.currentTimeMillis() - _start) / (60 * 1000);
    time = Math.max(time, 1);
    return (float) _queryCount / time;
  }

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
    LOG.info("Client.count()" + nodeShardsMap);
    final List<Integer> result = new ArrayList<Integer>();

    final long start = System.currentTimeMillis();
    final List<Thread> searchThreads = new ArrayList<Thread>();
    final Set<String> keySet = nodeShardsMap.keySet();
    for (final String node : keySet) {
      final ISearch searchNode = _nodeNames2Proxy.get(node);
      final List<String> shards = nodeShardsMap.get(node);
      final Runnable searchRunnable = new ResultCountThread(query, searchNode, shards, result, node);
      final Thread searchThread = new Thread(searchRunnable, node);
      searchThread.start();
      searchThreads.add(searchThread);
    }

    joinThreads(searchThreads);
    final long end = System.currentTimeMillis();
    LOG.info("Time for counting: " + (end - start) / 1000.0);

    int resultCount = 0;
    for (final Integer count : result) {
      resultCount += count.intValue();
    }

    return resultCount;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
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
        if (LOG.isDebugEnabled()) {
          startThread = System.currentTimeMillis();
        }
        final DocumentFrequenceWritable nodeDocFreqs = _searchNode.getDocFreqs(_query, _shards
            .toArray(new String[_shards.size()]));
        _docFreqs.addNumDocs(nodeDocFreqs.getNumDocs());
        _docFreqs.putAll(nodeDocFreqs.getAll());
        if (LOG.isDebugEnabled()) {
          final long endThread = System.currentTimeMillis();
          LOG.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
        }
      } catch (final IOException e) {
        // TODO we should Throw an Exception here since the results are
        // not correct..
        LOG.error("Cannot open searcher.", e);
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
        if (LOG.isDebugEnabled()) {
          startThread = System.currentTimeMillis();
        }
        final String[] shardsArray = _shards.toArray(new String[_shards.size()]);
        final HitsMapWritable shardToHits = _searchNode.search(_query, _docFreqs, shardsArray, _count);
        hits = shardToHits.getHits();
        if (LOG.isDebugEnabled()) {
          final long endThread = System.currentTimeMillis();
          LOG.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0
              + "sec. Result size was " + hits.getHits().size());
        }
      } catch (final IOException e) {
        LOG.error("Cannot open searcher.", e);
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
        if (LOG.isDebugEnabled()) {
          startThread = System.currentTimeMillis();
        }
        final int count = _searchNode.getResultCount(_query, _shards.toArray(new String[_shards.size()]));
        _result.add(count);
        if (LOG.isDebugEnabled()) {
          final long endThread = System.currentTimeMillis();
          LOG.debug("Wait for thread " + _node + " tooks " + (endThread - startThread) / 1000.0 + "sec.");
        }
      } catch (final IOException e) {
        LOG.error("Cannot open searcher, remove " + _node + " from connections.", e);
        _nodeNames2Proxy.remove(_node);
      }
    }

  }

}
