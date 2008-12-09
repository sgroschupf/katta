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
package net.sf.katta.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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

  protected final ZKClient _zkClient;

  private final IndexStateListener _indexStateListener = new IndexStateListener();
  private final IndexPathListener _indexPathChangeListener = new IndexPathListener();
  private final ShardNodeListener _shardNodeListener = new ShardNodeListener();

  protected final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();
  // TODO: jz remove node proxies if not needed anymore
  protected final Map<String, ISearch> _node2SearchProxyMap = new HashMap<String, ISearch>();

  protected final INodeSelectionPolicy _selectionPolicy;
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
    _hadoopConf.set("ipc.client.timeout", "2500");
    _hadoopConf.set("ipc.client.connect.max.retries", "2");
    // TODO jz: make configurable

    _selectionPolicy = policy;
    _zkClient = new ZKClient(config);
    try {
      _zkClient.getEventLock().lock();
      _zkClient.start(30000);

      List<String> indexes = _zkClient.subscribeChildChanges(ZkPathes.INDEXES, _indexPathChangeListener);
      addOrWatchNewIndexes(indexes);
    } finally {
      _zkClient.getEventLock().unlock();
    }
    _start = System.currentTimeMillis();
  }

  protected void updateSelectionPolicy(final String shardName, List<String> nodes) {
    List<String> connectedNodes = eastablishNodeProxiesIfNecessary(nodes);
    _selectionPolicy.update(shardName, connectedNodes);
  }

  private List<String> eastablishNodeProxiesIfNecessary(List<String> nodes) {
    List<String> connectedNodes = new ArrayList<String>(nodes);
    for (String node : nodes) {
      if (!_node2SearchProxyMap.containsKey(node)) {
        try {
          _node2SearchProxyMap.put(node, createNodeProxy(node));
        } catch (Exception e) {
          connectedNodes.remove(node);
          LOG.warn("could not create proxy for node '" + node + "' - " + e.getClass().getSimpleName());
        }
      }
    }
    return connectedNodes;
  }

  protected ISearch createNodeProxy(final String node) throws IOException {
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

  protected void removeIndexes(List<String> indexes) {
    for (String index : indexes) {
      List<String> shards = _indexToShards.remove(index);
      for (String shard : shards) {
        _selectionPolicy.remove(shard);
      }
    }
  }

  protected void addOrWatchNewIndexes(List<String> indexes) throws KattaException {
    for (String index : indexes) {
      String indexZkPath = ZkPathes.getIndexPath(index);
      IndexMetaData indexMetaData = _zkClient.readData(indexZkPath, IndexMetaData.class);
      if (isIndexSearchable(indexMetaData)) {
        addIndexForSearching(index, indexZkPath);
      } else {
        addIndexForWatching(indexZkPath);
      }
    }
  }

  protected void addIndexForWatching(final String indexZkPath) throws KattaException {
    _zkClient.subscribeDataChanges(indexZkPath, _indexStateListener);
  }

  protected void addIndexForSearching(String indexName, String indexZkPath) throws KattaException {
    final List<String> shards = _zkClient.getChildren(indexZkPath);
    _indexToShards.put(indexName, shards);
    for (final String shardName : shards) {
      List<String> nodes = _zkClient.subscribeChildChanges(ZkPathes.getShard2NodeRootPath(shardName),
          _shardNodeListener);
      updateSelectionPolicy(shardName, nodes);
    }
  }

  protected boolean isIndexSearchable(final IndexMetaData indexMetaData) {
    return indexMetaData.getState() == IndexMetaData.IndexState.DEPLOYED
        || indexMetaData.getState() == IndexMetaData.IndexState.REPLICATING;
  }

  public Hits search(final IQuery query, final String[] indexNames) throws KattaException {
    return search(query, indexNames, Integer.MAX_VALUE);
  }

  public Hits search(final IQuery query, final String[] indexNames, final int count) throws KattaException {
    final Map<String, List<String>> nodeShardsMap = getNode2ShardsMap(indexNames);
    final Hits result = new Hits();
    final DocumentFrequenceWritable docFreqs = getDocFrequencies(query, nodeShardsMap);

    List<NodeInteraction> nodeInteractions = new ArrayList<NodeInteraction>();
    for (final String node : nodeShardsMap.keySet()) {
      nodeInteractions.add(new SearchInteraction(node, nodeShardsMap, query, docFreqs, result, count));
    }
    execute(nodeInteractions);

    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    result.sort(count);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time for sorting: " + (System.currentTimeMillis() - start) + " ms");
    }
    _queryCount++;
    return result;
  }

  public int count(final IQuery query, final String[] indexNames) throws KattaException {
    final Map<String, List<String>> nodeShardsMap = getNode2ShardsMap(indexNames);
    final List<Integer> result = new ArrayList<Integer>();
    List<NodeInteraction> nodeInteractions = new ArrayList<NodeInteraction>();
    for (final String node : nodeShardsMap.keySet()) {
      nodeInteractions.add(new GetCountInteraction(node, nodeShardsMap, query, result));
    }
    execute(nodeInteractions);

    int resultCount = 0;
    for (final Integer count : result) {
      resultCount += count.intValue();
    }
    return resultCount;
  }

  public float getQueryPerMinute() {
    long time = (System.currentTimeMillis() - _start) / (60 * 1000);
    time = Math.max(time, 1);
    return (float) _queryCount / time;
  }

  private Map<String, List<String>> getNode2ShardsMap(final String[] indexNames) throws ShardAccessException {
    String[] indexesToSearchIn = indexNames;
    for (String indexName : indexNames) {
      if ("*".equals(indexName)) {
        // TODO jz: refactor to seperate methods but leave as deprecated
        indexesToSearchIn = new String[_indexToShards.keySet().size()];
        indexesToSearchIn = _indexToShards.keySet().toArray(indexesToSearchIn);
        break;
      }
    }

    List<String> shardsToSearchIn = getShardsToSearchIn(indexesToSearchIn);
    final Map<String, List<String>> nodeShardsMap = _selectionPolicy.createNode2ShardsMap(shardsToSearchIn);
    return nodeShardsMap;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
      Collection<ISearch> proxies = _node2SearchProxyMap.values();
      for (ISearch search : proxies) {
        RPC.stopProxy(search);
      }
    }
  }

  private List<String> getShardsToSearchIn(String[] indexNames) {
    List<String> shards = new ArrayList<String>();
    for (String index : indexNames) {
      shards.addAll(_indexToShards.get(index));
    }
    return shards;
  }

  private DocumentFrequenceWritable getDocFrequencies(final IQuery query, final Map<String, List<String>> node2ShardsMap)
      throws KattaException {
    DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();
    List<NodeInteraction> nodeInteractions = new ArrayList<NodeInteraction>();
    for (final String node : node2ShardsMap.keySet()) {
      nodeInteractions.add(new GetDocumentFrequencyInteraction(node, node2ShardsMap, query, docFreqs));
    }

    execute(nodeInteractions);
    return docFreqs;
  }

  private void execute(List<NodeInteraction> nodeInteractions) throws KattaException {
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    final List<Thread> interactionThreads = new ArrayList<Thread>(nodeInteractions.size());
    for (NodeInteraction nodeInteraction : nodeInteractions) {
      final Thread interactionThread = new Thread(nodeInteraction);
      interactionThreads.add(interactionThread);
      interactionThread.start();
      // TODO jz: use thread pool / Executor
    }

    try {
      for (final Thread thread : interactionThreads) {
        thread.join();
      }
    } catch (final InterruptedException e) {
      LOG.warn("Join for search threads interrupted.", e);
    }
    for (NodeInteraction nodeInteraction : nodeInteractions) {
      nodeInteraction.checkSuccess();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(nodeInteractions.get(0).getClass().getSimpleName() + " took " + (System.currentTimeMillis() - start)
          + " ms");
    }
  }

  protected class IndexStateListener implements IZkDataListener<IndexMetaData> {

    public void handleDataAdded(String dataPath, IndexMetaData data) throws KattaException {
      // handled through IndexPathListener
    }

    public void handleDataChange(String dataPath, IndexMetaData metaData) throws KattaException {
      final String indexName = ZkPathes.getName(dataPath);
      if (isIndexSearchable(metaData)) {
        addIndexForSearching(indexName, dataPath);
        _zkClient.unsubscribeDataChanges(dataPath, this);
      }
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
      Set<String> indexes = _indexToShards.keySet();
      addOrWatchNewIndexes(CollectionUtil.getListOfAdded(indexes, currentIndexes));

      List<String> removedIndexes = CollectionUtil.getListOfRemoved(indexes, currentIndexes);
      removeIndexes(removedIndexes);
    }
  }

  public MapWritable getDetails(final Hit hit) throws KattaException {
    return getDetails(hit, null);
  }

  public MapWritable getDetails(final Hit hit, final String[] fields) throws KattaException {
    Map<String, List<String>> node2ShardMap;
    String node = hit.getNode();
    List<String> shards = Arrays.asList(hit.getShard());
    if (_node2SearchProxyMap.containsKey(node)) {
      node2ShardMap = new HashMap<String, List<String>>(1);
      node2ShardMap.put(node, shards);
    } else {
      node2ShardMap = _selectionPolicy.createNode2ShardsMap(shards);
      node = node2ShardMap.keySet().iterator().next();
    }

    GetDetailsInteraction getDetailsInteraction = new GetDetailsInteraction(node, node2ShardMap, hit.getDocId(), fields);
    getDetailsInteraction.run();
    getDetailsInteraction.checkSuccess();
    return getDetailsInteraction.getDetails();
  }

  protected class ShardNodeListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentNodes) throws KattaException {
      LOG.info("got shard (" + parentPath + ") event: " + currentNodes);
      final String shardName = ZkPathes.getName(parentPath);

      // update shard2Nodes mapping
      updateSelectionPolicy(shardName, currentNodes);
    }
  }

  private class GetDocumentFrequencyInteraction extends NodeInteraction {

    private final IQuery _query;
    private final DocumentFrequenceWritable _docFreqs;

    public GetDocumentFrequencyInteraction(String node, Map<String, List<String>> node2ShardsMap, IQuery query,
        DocumentFrequenceWritable docFreqs) {
      super(node, node2ShardsMap);
      _query = query;
      _docFreqs = docFreqs;
    }

    @Override
    protected void doInteraction(ISearch search, String node, List<String> shards) throws IOException {
      final DocumentFrequenceWritable nodeDocFreqs = search.getDocFreqs(_query, shards
          .toArray(new String[shards.size()]));
      _docFreqs.addNumDocs(nodeDocFreqs.getNumDocs());
      _docFreqs.putAll(nodeDocFreqs.getAll());
    }
  }

  private class GetCountInteraction extends NodeInteraction {

    private final IQuery _query;
    private final List<Integer> _result;

    public GetCountInteraction(String node, Map<String, List<String>> node2ShardsMap, IQuery query, List<Integer> result) {
      super(node, node2ShardsMap);
      _query = query;
      _result = result;
    }

    @Override
    protected void doInteraction(ISearch search, String node, List<String> shards) throws IOException {
      final int count = search.getResultCount(_query, shards.toArray(new String[shards.size()]));
      _result.add(count);
    }
  }

  private class GetDetailsInteraction extends NodeInteraction {

    private final int _docId;
    private final String[] _fields;
    private MapWritable _details;

    public GetDetailsInteraction(String node, Map<String, List<String>> node2ShardsMap, int docId, String[] fields) {
      super(node, node2ShardsMap);
      _docId = docId;
      _fields = fields;
    }

    @Override
    protected void doInteraction(ISearch search, String node, List<String> shards) throws IOException {
      String shard = shards.get(0);
      if (_fields == null) {
        _details = search.getDetails(shard, _docId);
      } else {
        _details = search.getDetails(shard, _docId, _fields);
      }
    }

    public MapWritable getDetails() {
      return _details;
    }
  }

  private class SearchInteraction extends NodeInteraction {

    private final IQuery _query;
    private final int _count;
    private final DocumentFrequenceWritable _docFreqs;
    private final Hits _result;

    public SearchInteraction(String node, Map<String, List<String>> node2ShardsMap, IQuery query,
        DocumentFrequenceWritable docFreqs, Hits result, int count) {
      super(node, node2ShardsMap);
      _query = query;
      _docFreqs = docFreqs;
      _result = result;
      _count = count;
    }

    @Override
    protected void doInteraction(ISearch search, String node, List<String> shards) throws IOException {
      Hits hits;
      final String[] shardsArray = shards.toArray(new String[shards.size()]);
      final HitsMapWritable shardToHits = search.search(_query, _docFreqs, shardsArray, _count);
      hits = shardToHits.getHits();
      _result.addHits(hits.getHits());
      _result.addTotalHits(hits.size());
    }
  }

  /**
   * This class encapsulates an interaction with one or multiple node's in order
   * to query information for a set of shards.
   * 
   * Given the fact that shards a replicated about nodes, this class tries node
   * after node to get the desired information.
   */
  private abstract class NodeInteraction implements Runnable {

    private final String _node;
    private final Map<String, List<String>> _node2ShardsMap;
    private final List<String> _triedNodes = new ArrayList<String>(1);
    private int _tries = 0;
    private Exception _exception;

    public NodeInteraction(String node, Map<String, List<String>> node2ShardsMap) {
      _node = node;
      _node2ShardsMap = node2ShardsMap;
    }

    public final void run() {
      interact(_node, _node2ShardsMap);
    }

    protected final void interact(String node, Map<String, List<String>> node2ShardsMap) {
      List<String> shards = node2ShardsMap.get(node);
      try {
        _tries++;
        _triedNodes.add(node);
        ISearch searcher = _node2SearchProxyMap.get(node);
        try {
          if (searcher == null) {
            throw new IOException("node proxy for node " + node + " is not available any more");
          }
          long startTime = 0;
          if (LOG.isDebugEnabled()) {
            startTime = System.currentTimeMillis();
          }
          doInteraction(searcher, node, shards);
          if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + " with node " + node + " took "
                + (System.currentTimeMillis() - startTime) + " ms.");
          }
        } catch (IOException e) {
          if (_tries == 3) {
            throw new KattaException(getClass().getSimpleName() + " for shards " + shards + " failed. Tried nodes: "
                + _triedNodes);
          }
          LOG.warn(
              "failed to interact with node " + node + ". Try with other node(s) " + node2ShardsMap.keySet() + ".", e);
          Map<String, List<String>> node2ShardsMapForFailedNode = prepareRetry(node, shards);

          // execute the action again for every node
          for (String newNode : node2ShardsMapForFailedNode.keySet()) {
            // TODO jz: if more then one node we should spawn new
            // threads
            interact(newNode, node2ShardsMapForFailedNode);
          }
        }
      } catch (Exception e) {
        _exception = e;
      }
    }

    public void checkSuccess() throws KattaException {
      if (_exception != null) {
        if (_exception instanceof KattaException) {
          throw (KattaException) _exception;
        }
        throw new KattaException(getClass().getSimpleName() + " for shards " + _node2ShardsMap.get(_node)
            + " failed. Tried nodes: " + _triedNodes, _exception);
      }
    }

    private Map<String, List<String>> prepareRetry(String node, List<String> shards) throws ShardAccessException {
      // remove node
      _node2ShardsMap.remove(node);
      _node2SearchProxyMap.remove(node);
      _selectionPolicy.removeNode(node);

      // find new node(s) for the shards and add to global node2ShardMap
      Map<String, List<String>> node2ShardsMapForFailedNode = _selectionPolicy.createNode2ShardsMap(shards);
      for (String newNode : node2ShardsMapForFailedNode.keySet()) {
        List<String> newNodeShards = node2ShardsMapForFailedNode.get(newNode);
        if (!_node2ShardsMap.containsKey(newNode)) {
          _node2ShardsMap.put(newNode, newNodeShards);
        } else {
          _node2ShardsMap.get(newNode).addAll(newNodeShards);
        }
      }
      return node2ShardsMapForFailedNode;
    }

    protected abstract void doInteraction(ISearch search, String node, List<String> shards) throws IOException;
  }

}
