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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;

public class Client implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Client.class);
  private static final String[] ALL_INDICES = new String[] { "*" };

  protected final Set<String> _indicesToWatch = new HashSet<String>();
  protected final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();

  protected final INodeSelectionPolicy _selectionPolicy;
  private long _queryCount = 0;
  private final long _startupTime;

  private final ClientConfiguration _clientConfiguration;
  private final int _maxTryCount;
  protected InteractionProtocol _protocol;
  private INodeProxyManager _proxyManager;

  public Client(Class<? extends VersionedProtocol> serverClass) {
    this(serverClass, new DefaultNodeSelectionPolicy(), new ZkConfiguration());
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final ZkConfiguration config) {
    this(serverClass, new DefaultNodeSelectionPolicy(), config);
  }

  public Client(Class<? extends VersionedProtocol> serverClass, InteractionProtocol protocol) {
    this(serverClass, new DefaultNodeSelectionPolicy(), protocol, new ClientConfiguration());
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
    this(serverClass, policy, new InteractionProtocol(ZkKattaUtil.startZkClient(zkConfig, 60000), zkConfig),
            clientConfiguration);
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy,
          final InteractionProtocol protocol, ClientConfiguration clientConfiguration) {
    Set<String> keys = new HashSet<String>(clientConfiguration.getKeys());
    Configuration hadoopConf = new Configuration();
    synchronized (Configuration.class) {// fix for KATTA-146
      for (String key : keys) {
        // simply set all properties / adding non-hadoop properties shouldn't
        // hurt
        hadoopConf.set(key, clientConfiguration.getProperty(key));
      }
    }
    _proxyManager = new NodeProxyManager(serverClass, hadoopConf, policy);
    _selectionPolicy = policy;
    _protocol = protocol;
    _clientConfiguration = clientConfiguration;
    _maxTryCount = _clientConfiguration.getInt(ClientConfiguration.CLIENT_NODE_INTERACTION_MAXTRYCOUNT);

    List<String> indexList = _protocol.registerChildListener(this, PathDef.INDICES_METADATA, new IAddRemoveListener() {
      @Override
      public void removed(String name) {
        removeIndex(name);
      }

      @Override
      public void added(String name) {
        IndexMetaData indexMD = _protocol.getIndexMD(name);
        if (isIndexSearchable(indexMD)) {
          addIndexForSearching(indexMD);
        } else {
          addIndexForWatching(name);
        }
      }
    });
    LOG.info("indices=" + indexList);
    addOrWatchNewIndexes(indexList);
    _startupTime = System.currentTimeMillis();
  }

  public INodeSelectionPolicy getSelectionPolicy() {
    return _selectionPolicy;
  }

  public INodeProxyManager getProxyManager() {
    return _proxyManager;
  }

  public void setProxyCreator(INodeProxyManager proxyManager) {
    _proxyManager = proxyManager;
  }

  protected void removeIndexes(List<String> indexes) {
    for (String index : indexes) {
      removeIndex(index);
    }
  }

  protected void removeIndex(String index) {
    List<String> shards = _indexToShards.remove(index);
    if (shards != null) {
      for (String shard : shards) {
        _selectionPolicy.remove(shard);
        _protocol.unregisterChildListener(this, PathDef.SHARD_TO_NODES, shard);
      }
    } else {
      if (_indicesToWatch.contains(index)) {
        _protocol.unregisterDataChanges(this, PathDef.INDICES_METADATA, index);
      } else {
        LOG.warn("got remove event for index '" + index + "' but have no shards for it");
      }
    }
  }

  protected void addOrWatchNewIndexes(List<String> indexes) {
    for (String index : indexes) {
      IndexMetaData indexMD = _protocol.getIndexMD(index);
      if (indexMD != null) {// could be undeployed in meantime
        if (isIndexSearchable(indexMD)) {
          addIndexForSearching(indexMD);
        } else {
          addIndexForWatching(index);
        }
      }
    }
  }

  protected void addIndexForWatching(final String indexName) {
    _indicesToWatch.add(indexName);
    _protocol.registerDataListener(this, PathDef.INDICES_METADATA, indexName, new IZkDataListener() {
      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
        // handled through IndexPathListener
      }

      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
        IndexMetaData metaData = (IndexMetaData) data;
        if (isIndexSearchable(metaData)) {
          addIndexForSearching(metaData);
          _protocol.unregisterDataChanges(Client.this, dataPath);
        }
      }
    });
  }

  protected void addIndexForSearching(IndexMetaData indexMD) {
    final Set<Shard> shards = indexMD.getShards();
    List<String> shardNames = new ArrayList<String>();
    for (Shard shard : shards) {
      shardNames.add(shard.getName());
    }
    for (final String shardName : shardNames) {
      List<String> nodes = _protocol.registerChildListener(this, PathDef.SHARD_TO_NODES, shardName,
              new IAddRemoveListener() {
                @Override
                public void removed(String nodeName) {
                  LOG.info("shard '" + shardName + "' removed from node " + nodeName + "'");
                  Collection<String> shardNodes = new ArrayList<String>(_selectionPolicy.getShardNodes(shardName));
                  shardNodes.remove(nodeName);
                  _selectionPolicy.update(shardName, shardNodes);
                }

                @Override
                public void added(String nodeName) {
                  LOG.info("shard '" + shardName + "' added to node '" + nodeName + "'");
                  VersionedProtocol proxy = _proxyManager.getProxy(nodeName, true);
                  if (proxy != null) {
                    Collection<String> shardNodes = new ArrayList<String>(_selectionPolicy.getShardNodes(shardName));
                    shardNodes.add(nodeName);
                    _selectionPolicy.update(shardName, shardNodes);
                  }
                }
              });
      Collection<String> shardNodes = new ArrayList<String>(3);
      for (String node : nodes) {
        VersionedProtocol proxy = _proxyManager.getProxy(node, true);
        if (proxy != null) {
          shardNodes.add(node);
        }
      }
      _selectionPolicy.update(shardName, shardNodes);
    }
    _indexToShards.put(indexMD.getName(), shardNames);
  }

  protected boolean isIndexSearchable(final IndexMetaData indexMD) {
    if (indexMD.hasDeployError()) {
      return false;
    }
    // TODO jz check replication report ?
    return true;
  }

  // --------------- Distributed calls to servers ----------------------

  /**
   * Broadcast a method call to all indices. Return all the results in a
   * Collection.
   * 
   * @param <T>
   * @param timeout
   * @param shutdown
   * @param method
   *          The server's method to call.
   * @param shardArrayParamIndex
   *          Which parameter of the method call, if any, that should be
   *          replaced with the shards to search. This is an array of Strings,
   *          with a different value for each node / server. Pass in -1 to
   *          disable.
   * @param args
   *          The arguments to pass to the method when run on the server.
   * @return the results
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

    WorkQueue<T> workQueue = new WorkQueue<T>(_proxyManager, allShards, method, shardArrayParamIndex, args);

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

  // -------------------- Node management --------------------

  private Map<String, List<String>> getNode2ShardsMap(final String[] indexNames) throws KattaException {
    Collection<String> shardsToSearchIn = getShardsToSearchIn(indexNames);
    final Map<String, List<String>> nodeShardsMap = _selectionPolicy.createNode2ShardsMap(shardsToSearchIn);
    return nodeShardsMap;
  }

  private Collection<String> getShardsToSearchIn(String[] indexNames) throws KattaException {
    Collection<String> allShards = new HashSet<String>();
    for (String index : indexNames) {
      if ("*".equals(index)) {
        for (Collection<String> shardsOfIndex : _indexToShards.values()) {
          allShards.addAll(shardsOfIndex);
        }
        break;
      }
      List<String> shardsForIndex = _indexToShards.get(index);
      if (shardsForIndex != null) {
        allShards.addAll(shardsForIndex);
      } else {
        Pattern pattern = Pattern.compile(index);
        int matched = 0;
        for (String ind : _indexToShards.keySet()) {
          if (pattern.matcher(ind).matches()) {
            allShards.addAll(_indexToShards.get(ind));
            matched++;
          }
        }
        if (matched == 0) {
          LOG.warn("No shards found for index name/pattern: " + index);
        }
      }
    }
    if (allShards.isEmpty()) {
      throw new KattaException("Index [pattern(s)] '" + Arrays.toString(indexNames)
              + "' do not match to any deployed index: " + getIndices());
    }
    return allShards;
  }

  public double getQueryPerMinute() {
    double minutes = (System.currentTimeMillis() - _startupTime) / 60000.0;
    if (minutes > 0.0F) {
      return _queryCount / minutes;
    }
    return 0.0F;
  }

  public void close() {
    if (_protocol != null) {
      _protocol.unregisterComponent(this);
      _protocol.disconnect();
      _protocol = null;
      _proxyManager.shutdown();
    }
  }

  @Override
  public void disconnect() {
    // nothing to do - only connection to zk dropped. Proxies might still be
    // available.
  }

  @Override
  public void reconnect() {
    // TODO jz: re-read index information ?
  }

  public List<String> getIndices() {
    return new ArrayList<String>(_indexToShards.keySet());
  }

}
