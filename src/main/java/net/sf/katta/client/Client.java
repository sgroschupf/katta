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
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;

public class Client implements IShardProxyManager {

  protected final static Logger LOG = Logger.getLogger(Client.class);
  private static final String[] ALL_INDICES = new String[] { "*" };

  protected final ZkConfiguration _zkConfig;
  protected ZkClient _zkClient;
  protected final Class<? extends VersionedProtocol> _serverClass;

  private final IndexStateListener _indexStateListener = new IndexStateListener();
  private final IndexPathListener _indexPathChangeListener = new IndexPathListener();
  private final ShardNodeListener _shardNodeListener = new ShardNodeListener();

  protected final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();
  protected final Map<String, VersionedProtocol> _node2ProxyMap = new HashMap<String, VersionedProtocol>();

  protected final INodeSelectionPolicy _selectionPolicy;
  private long _queryCount = 0;
  private final long _startupTime;

  private Configuration _hadoopConf = new Configuration();
  private final ClientConfiguration _clientConfiguration;
  private final int _maxTryCount;

  public Client(Class<? extends VersionedProtocol> serverClass) {
    this(serverClass, new DefaultNodeSelectionPolicy(), new ZkConfiguration());
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final ZkConfiguration config) {
    this(serverClass, new DefaultNodeSelectionPolicy(), config);
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy nodeSelectionPolicy) {
    this(serverClass, nodeSelectionPolicy, new ZkConfiguration());
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy,
      final ZkConfiguration zkConfig) {
    this(serverClass, policy, zkConfig, new ClientConfiguration());
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy,
      final ZkConfiguration zkConfig, ClientConfiguration clientConfiguration) {
    Set<String> keys = clientConfiguration.getKeys();
    for (String key : keys) {
      // simply set all properties / adding non-hadoop properties shouldn't hurt
      _hadoopConf.set(key, clientConfiguration.getProperty(key));
    }

    _serverClass = serverClass;
    _selectionPolicy = policy;
    _zkConfig = zkConfig;
    _clientConfiguration = clientConfiguration;
    _maxTryCount = _clientConfiguration.getInt(ClientConfiguration.CLIENT_NODE_INTERACTION_MAXTRYCOUNT);

    // TODO PVo should we really start a new ZkClient here?
    _zkClient = ZkKattaUtil.startZkClient(zkConfig, 60000);
    String indicesPath = zkConfig.getZKIndicesPath();
    List<String> indexList = _zkClient.subscribeChildChanges(indicesPath, _indexPathChangeListener);
    LOG.info("children=" + indexList);
    addOrWatchNewIndexes(indexList);
    _startupTime = System.currentTimeMillis();
  }

  // --------------- Proxy handling ----------------------

  protected void updateSelectionPolicy(final String shardName, List<String> nodes) {
    List<String> connectedNodes = eastablishNodeProxiesIfNecessary(nodes);
    _selectionPolicy.update(shardName, connectedNodes);
  }

  private List<String> eastablishNodeProxiesIfNecessary(List<String> nodes) {
    List<String> connectedNodes = new ArrayList<String>(nodes);
    for (String node : nodes) {
      if (!_node2ProxyMap.containsKey(node)) {
        try {
          _node2ProxyMap.put(node, createNodeProxy(node));
        } catch (Exception e) {
          connectedNodes.remove(node);
          LOG.warn("Could not create proxy for node '" + node + "' - " + e.getClass().getSimpleName());
        }
      }
    }
    return connectedNodes;
  }

  protected VersionedProtocol createNodeProxy(final String node) throws IOException {
    LOG.debug("creating proxy for node: " + node);

    String[] hostName_port = node.split(":");
    if (hostName_port.length != 2) {
      throw new RuntimeException("invalid node name format '" + node
              + "' (It should be a host name with a port number devided by a ':')");
    }
    final String hostName = hostName_port[0];
    final String port = hostName_port[1];
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, Integer.parseInt(port));
    VersionedProtocol proxy = RPC.getProxy(_serverClass, 0L, inetSocketAddress, _hadoopConf);
    LOG.debug(String.format("Created a proxy %s for %s:%s %s", Proxy.getInvocationHandler(proxy), hostName, port,
            inetSocketAddress));
    return proxy;
  }

  protected void removeIndexes(List<String> indexes) {
    for (String index : indexes) {
      List<String> shards = _indexToShards.remove(index);
      for (String shard : shards) {
        _selectionPolicy.remove(shard);
      }
    }
  }

  protected void addOrWatchNewIndexes(List<String> indexes) {
    for (String index : indexes) {
      String indexZkPath = _zkConfig.getZKIndexPath(index);
      IndexMetaData indexMetaData = _zkClient.readData(indexZkPath);
      if (isIndexSearchable(indexMetaData)) {
        addIndexForSearching(index, indexZkPath);
      } else {
        addIndexForWatching(indexZkPath);
      }
    }
  }

  protected void addIndexForWatching(final String indexZkPath) {
    _zkClient.subscribeDataChanges(indexZkPath, _indexStateListener);
  }

  protected void addIndexForSearching(String indexName, String indexZkPath) {
    final List<String> shards = _zkClient.getChildren(indexZkPath);
    _indexToShards.put(indexName, shards);
    for (final String shardName : shards) {
      String shardToNodePath = _zkConfig.getZKShardToNodePath(shardName);
      List<String> nodes = _zkClient.subscribeChildChanges(shardToNodePath, _shardNodeListener);
      updateSelectionPolicy(shardName, nodes);
    }
  }

  protected boolean isIndexSearchable(final IndexMetaData indexMetaData) {
    return indexMetaData.getState() == IndexMetaData.IndexState.DEPLOYED
            || indexMetaData.getState() == IndexMetaData.IndexState.REPLICATING;
  }

  // --------------- Distributed calls to servers ----------------------

  /**
   * Broadcast a method call to all indices. Return all the results in a
   * Collection.
   * 
   * @param method
   *          The server's method to call.
   * @param shardArrayParamIndex
   *          Which parameter of the method call, if any, that should be
   *          replaced with the shards to search. This is an array of Strings,
   *          with a different value for each node / server. Pass in -1 to
   *          disable.
   * @param args
   *          The arguments to pass to the method when run on the server.
   * @return
   * @throws KattaException
   */
  public <T> ClientResult<T> broadcastToAll(long timeout, boolean shutdown, Method method, int shardArrayParamIndex,
          Object... args) throws KattaException {
    return broadcastToAll(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, args);
  }

  public <T> ClientResult<T> broadcastToAll(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex,
          Object... args) throws KattaException {
    return broadcastToShards(resultPolicy, method, shardArrayParamIndex, null, args);
  }

  public <T> ClientResult<T> broadcastToIndices(long timeout, boolean shutdown, Method method, int shardArrayIndex,
          String[] indices, Object... args) throws KattaException {
    return broadcastToIndices(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayIndex, indices, args);
  }

  public <T> ClientResult<T> broadcastToIndices(IResultPolicy<T> resultPolicy, Method method, int shardArrayIndex,
          String[] indices, Object... args) throws KattaException {
    if (indices == null) {
      indices = ALL_INDICES;
    }
    Map<String, List<String>> nodeShardsMap = getNode2ShardsMap(indices);
    if (nodeShardsMap.values().isEmpty()) {
      throw new KattaException("No shards for indices: "
              + (indices != null ? Arrays.asList(indices).toString() : "null"));
    }
    return broadcastInternal(resultPolicy, method, shardArrayIndex, nodeShardsMap, args);
  }

  public <T> ClientResult<T> singlecast(long timeout, boolean shutdown, Method method, int shardArrayParamIndex,
          String shard, Object... args) throws KattaException {
    return singlecast(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, shard, args);
  }

  public <T> ClientResult<T> singlecast(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex,
          String shard, Object... args) throws KattaException {
    List<String> shards = new ArrayList<String>();
    shards.add(shard);
    return broadcastToShards(resultPolicy, method, shardArrayParamIndex, shards, args);
  }

  public <T> ClientResult<T> broadcastToShards(long timeout, boolean shutdown, Method method, int shardArrayParamIndex,
          List<String> shards, Object... args) throws KattaException {
    return broadcastToShards(new ResultCompletePolicy<T>(timeout, shutdown), method, shardArrayParamIndex, shards, args);
  }

  public <T> ClientResult<T> broadcastToShards(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex,
          List<String> shards, Object... args) throws KattaException {
    if (shards == null) {
      // If no shards specified, search all shards.
      shards = new ArrayList<String>();
      for (List<String> indexShards : _indexToShards.values()) {
        shards.addAll(indexShards);
      }
    }
    final Map<String, List<String>> nodeShardsMap = _selectionPolicy.createNode2ShardsMap(shards);
    if (nodeShardsMap.values().isEmpty()) {
      throw new KattaException("No shards selected: " + shards);
    }
    return broadcastInternal(resultPolicy, method, shardArrayParamIndex, nodeShardsMap, args);
  }

  private <T> ClientResult<T> broadcastInternal(IResultPolicy<T> resultPolicy, Method method, int shardArrayParamIndex,
          Map<String, List<String>> nodeShardsMap, Object... args) {
    _queryCount++;
    /*
     * Validate inputs.
     */
    if (method == null || args == null) {
      throw new IllegalArgumentException("Null method or args!");
    }
    Class<?>[] types = method.getParameterTypes();
    if (args.length != types.length) {
      throw new IllegalArgumentException("Wrong number of args: found " + args.length + ", expected " + types.length
              + "!");
    }
    for (int i = 0; i < args.length; i++) {
      if (args[i] != null) {
        Class<?> from = args[i].getClass();
        Class<?> to = types[i];
        if (!to.isAssignableFrom(from) && !(from.isPrimitive() || to.isPrimitive())) {
          // Assume autoboxing will work.
          throw new IllegalArgumentException("Incorrect argument type for param " + i + ": expected " + types[i] + "!");
        }
      }
    }
    if (shardArrayParamIndex > 0) {
      if (shardArrayParamIndex >= types.length) {
        throw new IllegalArgumentException("shardArrayParamIndex out of range!");
      }
      if (!(types[shardArrayParamIndex]).equals(String[].class)) {
        throw new IllegalArgumentException("shardArrayParamIndex parameter (" + shardArrayParamIndex
                + ") is not of type String[]!");
      }
    }
    if (LOG.isTraceEnabled()) {
      for (Map.Entry<String, List<String>> e : _indexToShards.entrySet()) {
        LOG.trace("_indexToShards " + e.getKey() + " --> " + e.getValue().toString());
      }
      for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
        LOG.trace("broadcast using " + e.getKey() + " --> " + e.getValue().toString());
      }
      LOG.trace("selection policy = " + _selectionPolicy);
    }

    /*
     * Make RPC calls to all nodes in parallel.
     */
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    /*
     * We don't know what _selectionPolicy built, and multiple threads may write
     * to map if IO errors occur. This map might be shared across multiple calls
     * also. So make a copy and synchronize it.
     */
    Map<String, List<String>> nodeShardMapCopy = new HashMap<String, List<String>>();
    Set<String> allShards = new HashSet<String>();
    for (Map.Entry<String, List<String>> e : nodeShardsMap.entrySet()) {
      nodeShardMapCopy.put(e.getKey(), new ArrayList<String>(e.getValue()));
      allShards.addAll(e.getValue());
    }
    nodeShardsMap = Collections.synchronizedMap(nodeShardMapCopy);
    nodeShardMapCopy = null;

    WorkQueue<T> workQueue = new WorkQueue<T>(this, allShards, method, shardArrayParamIndex, args);

    
    for (String node : nodeShardsMap.keySet()) {
      workQueue.execute(node, nodeShardsMap, 1, _maxTryCount);
    }

    ClientResult<T> results = workQueue.getResults(resultPolicy);

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("broadcast(%s(%s), %s) took %d msec for %s", method.getName(), args, nodeShardsMap,
              (System.currentTimeMillis() - start), results != null ? results : "null"));
    }
    return results;
  }

  // --------------------- IShardManager ----------------------------

  // NodeInteractions will use these methods.

  public VersionedProtocol getProxy(String node) {
    return _node2ProxyMap.get(node);
  }

  public void nodeFailed(String node, Throwable t) {
    _node2ProxyMap.remove(node);
    _selectionPolicy.removeNode(node);
  }

  public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
    return _selectionPolicy.createNode2ShardsMap(shards);
  }

  // -------------------- Node management --------------------

  private Map<String, List<String>> getNode2ShardsMap(final String[] indexNames) throws KattaException {
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

  private List<String> getShardsToSearchIn(String[] indexNames) throws KattaException {
    List<String> shards = new ArrayList<String>();
    for (String index : indexNames) {
      List<String> theseShards = _indexToShards.get(index);
      if (theseShards != null) {
        List<String> shardsForIndex = _indexToShards.get(index);
        if (shardsForIndex == null) {
          throw new KattaException("Index '" + index + "' not deployed on any shard.");
        }
        shards.addAll(shardsForIndex);
      } else {
        LOG.warn("No shards found for index " + index);
      }
    }
    return shards;
  }

  public double getQueryPerMinute() {
    double minutes = (System.currentTimeMillis() - _startupTime) / 60000.0;
    if (minutes > 0.0F) {
      return _queryCount / minutes;
    }
    return 0.0F;
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
      Collection<VersionedProtocol> proxies = _node2ProxyMap.values();
      for (VersionedProtocol search : proxies) {
        RPC.stopProxy(search);
      }
      _zkClient = null;
    }
  }

  protected class IndexStateListener implements IZkDataListener {

    public void handleDataDeleted(String dataPath) throws KattaException {
      // handled through IndexPathListener
    }

    public IndexMetaData createWritable() {
      return new IndexMetaData();
    }

    @Override
    public void handleDataChange(String dataPath, Serializable data) throws Exception {
      IndexMetaData metaData = (IndexMetaData) data;
      final String indexName = _zkConfig.getZKName(dataPath);
      if (isIndexSearchable(metaData)) {
        addIndexForSearching(indexName, dataPath);
        _zkClient.unsubscribeDataChanges(dataPath, this);
      }
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

  protected class ShardNodeListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentNodes) throws KattaException {
      LOG.info("got shard (" + parentPath + ") event: " + currentNodes);
      final String shardName = _zkConfig.getZKName(parentPath);

      // update shard2Nodes mapping
      updateSelectionPolicy(shardName, currentNodes);
    }

  }

}
