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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;

public class Client implements IShardProxyManager, ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Client.class);
  private static final String[] ALL_INDICES = new String[] { "*" };

  protected final Class<? extends VersionedProtocol> _serverClass;

  protected final Map<String, List<String>> _indexToShards = new HashMap<String, List<String>>();
  protected final Map<String, VersionedProtocol> _node2ProxyMap = new HashMap<String, VersionedProtocol>();

  protected final INodeSelectionPolicy _selectionPolicy;
  private long _queryCount = 0;
  private final long _startupTime;

  private Configuration _hadoopConf = new Configuration();
  private final ClientConfiguration _clientConfiguration;
  private final int _maxTryCount;
  protected InteractionProtocol _protocol;

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
    this(serverClass, policy, new InteractionProtocol(ZkKattaUtil.startZkClient(zkConfig, 60000), zkConfig),
            clientConfiguration);
  }

  public Client(Class<? extends VersionedProtocol> serverClass, final INodeSelectionPolicy policy,
          final InteractionProtocol protocol, ClientConfiguration clientConfiguration) {
    Set<String> keys = new HashSet<String>(clientConfiguration.getKeys());
    for (String key : keys) {
      // simply set all properties / adding non-hadoop properties shouldn't hurt
      _hadoopConf.set(key, clientConfiguration.getProperty(key));
    }

    _serverClass = serverClass;
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

  // --------------- Proxy handling ----------------------

  /**
   * @param node
   * @return true if connection could be or is established
   */
  protected boolean eastablishNodeProxyIfNecessary(String node) {
    try {
      synchronized (_node2ProxyMap) {
        if (!_node2ProxyMap.containsKey(node)) {
          _node2ProxyMap.put(node, createNodeProxy(node));
        }
        return true;
      }
    } catch (Exception e) {
      LOG.warn("Could not create proxy for node '" + node + "' - " + e.getClass().getSimpleName() + ": "
              + e.getMessage());
      return false;
    }
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
      removeIndex(index);
    }
  }

  protected void removeIndex(String index) {
    List<String> shards = _indexToShards.remove(index);
    for (String shard : shards) {
      _selectionPolicy.remove(shard);
    }
  }

  protected void addOrWatchNewIndexes(List<String> indexes) {
    for (String index : indexes) {
      IndexMetaData indexMD = _protocol.getIndexMD(index);
      if (isIndexSearchable(indexMD)) {
        addIndexForSearching(indexMD);
      } else {
        addIndexForWatching(index);
      }
    }
  }

  protected void addIndexForWatching(final String indexName) {
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
          _protocol.unregisterDataChanges(Client.this, dataPath, this);
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
    _indexToShards.put(indexMD.getName(), shardNames);
    for (final String shardName : shardNames) {
      List<String> nodes = _protocol.registerChildListener(this, PathDef.SHARD_TO_NODES, shardName,
              new IAddRemoveListener() {
                @Override
                public void removed(String name) {
                  LOG.info("shard " + shardName + ": removed node " + name);
                  Collection<String> shardNodes = new ArrayList<String>(_selectionPolicy.getShardNodes(shardName));
                  shardNodes.remove(name);
                  _selectionPolicy.update(shardName, shardNodes);
                }

                @Override
                public void added(String name) {
                  LOG.info("shard " + shardName + ": added node " + name);
                  boolean connectionEstablished = eastablishNodeProxyIfNecessary(name);
                  if (connectionEstablished) {
                    Collection<String> shardNodes = new ArrayList<String>(_selectionPolicy.getShardNodes(shardName));
                    shardNodes.add(name);
                    _selectionPolicy.update(shardName, shardNodes);
                  }
                }
              });
      Collection<String> shardNodes = new ArrayList<String>(3);
      for (String node : nodes) {
        boolean connectionEstablished = eastablishNodeProxyIfNecessary(node);
        if (connectionEstablished) {
          shardNodes.add(node);
        }
      }
      _selectionPolicy.update(shardName, shardNodes);
    }
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

  // --------------------- IShardProxyManager ----------------------------

  // NodeInteractions will use these methods.

  public VersionedProtocol getProxy(String node) {
    return _node2ProxyMap.get(node);
  }

  public void nodeFailed(String node, Throwable t) {
    // TODO jz: maybe we should inspect the exception to identify if its really
    // an proxy problem ?
    synchronized (_node2ProxyMap) {
      VersionedProtocol proxy = _node2ProxyMap.remove(node);
      RPC.stopProxy(proxy);
    }
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
    if (_protocol != null) {
      _protocol.disconnect();
      Collection<VersionedProtocol> proxies = _node2ProxyMap.values();
      for (VersionedProtocol search : proxies) {
        RPC.stopProxy(search);
      }
      _protocol = null;
    }
  }

  @Override
  public void disconnect() {
    // nothing to do - only connection to zk dropped. Proxies might still be
    // availible.
  }

  @Override
  public void reconnect() {
    // TODO jz: re-read index information ?
  }

}
