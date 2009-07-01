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
package net.sf.katta.master;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class DistributeShardsThread extends Thread {

  protected final static Logger LOG = Logger.getLogger(DistributeShardsThread.class);

  private final ZkConfiguration _conf;
  private final ZKClient _zkClient;
  private final IDeployPolicy _deployPolicy;
  private final long _safeModeMaxTime;

  private final StatusUpdate _statusUpdate = new StatusUpdate();
  private final UpdateLock _updateLock = new UpdateLock();

  private boolean _safeMode;

  protected final List<IndexStateListener> _indexStateListeners = new CopyOnWriteArrayList<IndexStateListener>();

  public DistributeShardsThread(final ZKClient zkClient, final IDeployPolicy deployPolicy, final long safeModeMaxTime) {
    this(zkClient, deployPolicy, safeModeMaxTime, true);
  }

  public DistributeShardsThread(final ZKClient zkClient, final IDeployPolicy deployPolicy, final long safeModeMaxTime,
          boolean isDaemon) {
    setDaemon(true);
    setName(getClass().getSimpleName());
    _deployPolicy = deployPolicy;
    _zkClient = zkClient;
    _conf = zkClient.getConfig();
    // try {
    // _zkClient.getEventLock().lock();
    // _zkClient.subscribeReconnects(this);
    // } finally {
    // _zkClient.getEventLock().unlock();
    // }
    _safeModeMaxTime = safeModeMaxTime;
  }

  public void updateIndexes(final Collection<String> indexes) {
    _statusUpdate.updateIndexes(indexes);
    try {
      _updateLock.lock();
      _updateLock.getUpdatedCondition().signal();
    } finally {
      _updateLock.unlock();
    }
  }

  public void updateNodes(final Collection<String> nodes) {
    _statusUpdate.updateNodes(nodes);
    try {
      _updateLock.lock();
      _updateLock.getUpdatedCondition().signal();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      _updateLock.unlock();
    }
  }

  public boolean isInSafeMode() {
    return _safeMode;
  }

  @Override
  public void run() {
    try {
      LOG.info("starting...");
      Set<String> liveNodes = new HashSet<String>();
      Set<String> liveIndexes = new HashSet<String>();
      runInSafeMode(liveNodes, liveIndexes);

      try {
        doStartupCheck(liveNodes);
      } catch (final Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          throw (InterruptedException) e.getCause();
        }
        LOG.error("Failed to execute startup check {" + toString(liveIndexes, liveNodes) + "}", e);
      }

      while (true) {
        // sleep(3000);
        Set<String> updatedNodes = null;
        Set<String> updatedIndexes = null;
        try {
          Set<String> unbalancedIndexes = getUnbalancedIndexes(_statusUpdate.getNodes().size());
          while (!(_statusUpdate.hasChanges(liveIndexes, liveNodes) || !unbalancedIndexes.isEmpty())
                  || _statusUpdate.getNodes().isEmpty()) {
            if (_statusUpdate.getNodes().isEmpty()) {
              LOG.warn("no nodes connected - delaying update");
              // jz: if connected nodes in under a certain threshold go in
              // safe-mode?
            }
            try {
              _updateLock.lock();
              _updateLock.getUpdatedCondition().await();
            } finally {
              _updateLock.unlock();
            }

            unbalancedIndexes = getUnbalancedIndexes(_statusUpdate.getNodes().size());
            // TODO jz: wait x ms and if nothing happens rebalance ??
          }
          LOG.info("processing of update started...");
          updatedIndexes = new HashSet<String>(_statusUpdate.getIndexes());
          updatedNodes = new HashSet<String>(_statusUpdate.getNodes());

          // get deltas
          final Set<String> addedIndexes = CollectionUtil.getSetOfAdded(liveIndexes, updatedIndexes);
          final Set<String> removedIndexes = CollectionUtil.getSetOfRemoved(liveIndexes, updatedIndexes);
          final Set<String> addedNodes = CollectionUtil.getSetOfAdded(liveNodes, updatedNodes);
          if (liveNodes.size() > updatedNodes.size()) {
            LOG.info("following nodes disconnected: " + CollectionUtil.getSetOfRemoved(liveNodes, updatedNodes));
            // jz: we don't need to handle removed nodes explicitly since we
            // already handle unbalanced indexes
          }
          liveIndexes = updatedIndexes;
          liveNodes = updatedNodes;

          // first free up space
          handleRemovedIndexes(removedIndexes);
          // "save" existing indexes
          distributeIndexes(unbalancedIndexes, IndexState.REPLICATING, liveNodes);
          // add new indexes
          distributeIndexes(addedIndexes, IndexState.DEPLOYING, liveNodes);
          // maybe rebalance
          handleAddedNodes(addedNodes);
        } catch (final InterruptedException e) {
          throw e;
        } catch (final Exception e) {
          if (e.getCause() instanceof InterruptedException) {
            throw (InterruptedException) e.getCause();
          }
          LOG.error("Failed to execute shard update to {" + toString(updatedIndexes, updatedNodes) + "}", e);
        }
        LOG.info("processing of update finsihed!");
        sleep(1000);
      }
    } catch (final InterruptedException e) {
      // sg: in case we shutdown this thread we need to make sure all index
      // listener unsubscribe zookeeper notification to make sure zookeeper is
      // able to shutdown cleanly
      for (final IndexStateListener listener : _indexStateListeners) {
        listener.unsubscribeShardEvents();
      }
      LOG.info("manage shard thread stopped");
    }
  }

  private void runInSafeMode(Set<String> liveNodes, Set<String> liveIndexes) throws InterruptedException {
    _safeMode = true;
    try {
      while (_statusUpdate.lastChangeTimeStamp() + _safeModeMaxTime > System.currentTimeMillis()
              || _statusUpdate.getNodes().isEmpty() || areNodesConnecting(_statusUpdate.getNodes())) {
        LOG.info("SAFE MODE: No nodes available or state unstable within the last " + _safeModeMaxTime + " ms.");
        try {
          _updateLock.lock();
          _updateLock.getUpdatedCondition().await(_safeModeMaxTime, TimeUnit.MILLISECONDS);
        } finally {
          _updateLock.unlock();
        }
      }
      liveNodes.addAll(_statusUpdate.getNodes());
      liveIndexes.addAll(_statusUpdate.getIndexes());
      LOG.info("SAFE MODE: leaving safe mode with " + liveNodes.size() + " connected nodes");
    } finally {
      _safeMode = false;

    }
  }

  private boolean areNodesConnecting(Set<String> nodes) {
    for (String node : nodes) {
      NodeMetaData nodeMetaData = new NodeMetaData();
      try {
        _zkClient.readData(_conf.getZKNodePath(node), nodeMetaData);
        if (nodeMetaData.getState() == NodeState.STARTING || nodeMetaData.getState() == NodeState.RECONNECTING) {
          return true;
        }
      } catch (KattaException e) {
        // on startup there could be some node data removals and addings since
        // the "old" ephemerals have to be cleared
        LOG.warn("failed to load node data");
      }
    }
    return false;
  }

  private String toString(final Set<String> updatedIndexes, final Set<String> updatedNodes) {
    return "indexes: " + updatedIndexes + " | nodes: " + updatedNodes;
  }

  private void doStartupCheck(Set<String> liveNodes) throws KattaException {
    LOG.info("do startup check...");
    distributeIndexes(getIndexesInState(IndexState.DEPLOYING), IndexState.DEPLOYING, liveNodes);
    distributeIndexes(getIndexesInState(IndexState.REPLICATING), IndexState.REPLICATING, liveNodes);
    distributeIndexes(getIndexesInState(IndexState.ANNOUNCED), IndexState.DEPLOYING, liveNodes);
    distributeIndexes(getUnbalancedIndexes(liveNodes.size()), IndexState.REPLICATING, liveNodes);

    // TODO jz: check namespace structure ??
    LOG.info("startup check done");
  }

  private Set<String> getIndexesInState(IndexState indexState) throws KattaException {
    final Set<String> indexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(_conf.getZKIndicesPath())) {
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(_conf.getZKIndexPath(index), indexMetaData);
      if (indexMetaData.getState() == indexState) {
        indexes.add(index);
      }
    }
    if (!indexes.isEmpty()) {
      LOG.info("found following indexes in state '" + indexState + "': " + indexes);
    }
    return indexes;
  }

  private Set<String> getUnbalancedIndexes(int nodeCount) throws KattaException {
    final Set<String> unbalancedIndexes = new HashSet<String>();
    final Set<String> underreplicatedIndexes = getUnderreplicatedIndexes(nodeCount);
    final Set<String> overreplicatedIndexes = getOverreplicatedIndexes();
    if (!underreplicatedIndexes.isEmpty()) {
      LOG.info("found following underreplicated indexes: " + underreplicatedIndexes);
    }
    if (!overreplicatedIndexes.isEmpty()) {
      LOG.info("found following overreplicated indexes: " + overreplicatedIndexes);
    }

    unbalancedIndexes.addAll(overreplicatedIndexes);
    unbalancedIndexes.addAll(underreplicatedIndexes);
    return unbalancedIndexes;
  }

  private Set<String> getUnderreplicatedIndexes(int nodeCount) throws KattaException {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(_conf.getZKIndicesPath())) {
      final String indexZkPath = _conf.getZKIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, indexMetaData);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        int desiredReplicationCount = indexMetaData.getReplicationLevel();
        int minimalReplicationCount = getMinimalReplicationCount(indexZkPath, desiredReplicationCount);
        if (minimalReplicationCount < desiredReplicationCount) {
          if (minimalReplicationCount >= nodeCount) {
            LOG.warn("found index '" + index
                    + "' underreplicated but skip replicating since not enough nodes connected");
          } else {
            underreplicatedIndexes.add(index);
          }
        }
      }
    }
    return underreplicatedIndexes;
  }

  private Set<String> getOverreplicatedIndexes() throws KattaException {
    final Set<String> overreplicatedIndexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(_conf.getZKIndicesPath())) {
      final String indexZkPath = _conf.getZKIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, indexMetaData);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        if (isOverReplicated(indexZkPath, indexMetaData)) {
          overreplicatedIndexes.add(index);
        }
      }
    }
    return overreplicatedIndexes;
  }

  private int getMinimalReplicationCount(final String indexZkPath, int desiredReplicationCount) throws KattaException {
    int minimalReplicationCount = desiredReplicationCount;
    final List<String> shards = _zkClient.getChildren(indexZkPath);
    final Map<String, List<String>> shard2NodesMap = readShard2NodesMapFromZk(_zkClient, new HashSet<String>(shards));
    for (final String shard : shards) {
      final int servingNodes = shard2NodesMap.get(shard).size();
      if (servingNodes < minimalReplicationCount) {
        minimalReplicationCount = servingNodes;
      }
    }
    return minimalReplicationCount;
  }

  private boolean isOverReplicated(final String indexZkPath, final IndexMetaData indexMetaData) throws KattaException {
    final List<String> shards = _zkClient.getChildren(indexZkPath);
    final Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, new HashSet<String>(
            shards));
    for (final String shard : shards) {
      final int servingNodes = currentShard2NodesMap.get(shard).size();
      if (servingNodes > indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
  }

  private void handleRemovedIndexes(final Set<String> removedIndexes) throws KattaException {
    if (removedIndexes.isEmpty()) {
      return;
    }
    LOG.info("remove indexes: " + removedIndexes);
    // TODO: jz what if index is in replicate state ?

    // iterates through all nodes and removes the assigned shards from index
    for (final String indexName : removedIndexes) {
      final List<String> nodes = _zkClient.getKnownNodes();
      for (final String node : nodes) {
        final List<String> shards = _zkClient.getChildren(_conf.getZKNodeToShardPath(node));
        for (final String shard : shards) {
          final String node2ShardPath = _conf.getZKNodeToShardPath(node, shard);
          final AssignedShard shardWritable = new AssignedShard();
          _zkClient.readData(node2ShardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _zkClient.delete(node2ShardPath);
          }
        }
      }
    }
  }

  private void handleAddedNodes(final Set<String> addedNodes) {
    if (addedNodes.isEmpty()) {
      return;
    }
    LOG.info("add nodes: " + addedNodes);

    // regulate over- or underreplicated indexes is already done

    // TODO jz: rebalance nodes load ?
  }

  private void distributeIndexes(final Set<String> affectedIndexes, final IndexState state, Set<String> liveNodes)
          throws KattaException {
    if (affectedIndexes.isEmpty()) {
      return;
    }

    LOG.info(state.name().toLowerCase() + " following indexes:  " + affectedIndexes);
    for (final String index : affectedIndexes) {
      final String indexZkPath = _conf.getZKIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      try {
        _zkClient.readData(_conf.getZKIndexPath(index), indexMetaData);
        LOG.info(state.name().toLowerCase() + " shards for index '" + index + "' (" + indexMetaData.getState() + ")");

        final Map<String, AssignedShard> shard2AssignedShardMap = readShardsFromFs(index, indexMetaData);
        final Set<String> indexShards = shard2AssignedShardMap.keySet();
        LOG.info("Found shards '" + indexShards + "' for index '" + index + "'");

        indexMetaData.setState(state);
        _zkClient.writeData(indexZkPath, indexMetaData);
        distributeIndexShards(index, indexMetaData, indexShards, shard2AssignedShardMap, liveNodes);
      } catch (final Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          throw new KattaException("Distribution of shards was interrupted", e.getCause());
        }
        LOG.error("could not deploy index '" + index + "'", e);
        _zkClient.readData(indexZkPath, indexMetaData);
        indexMetaData.setState(IndexState.ERROR, StringUtils.stringifyException(e));
        _zkClient.writeData(indexZkPath, indexMetaData);
      }
    }
  }

  private void distributeIndexShards(final String index, final IndexMetaData indexMD, final Set<String> indexShards,
          final Map<String, AssignedShard> shard2AssignedShardMap, Collection<String> liveNodes) throws KattaException {
    // cleanup/undeploy failed shards
    for (final String shard : indexShards) {
      final String shard2ErrorRootPath = _conf.getZKShardToErrorPath(shard);
      if (_zkClient.exists(shard2ErrorRootPath)) {
        final List<String> nodesWithFailedShard = _zkClient.getChildren(shard2ErrorRootPath);
        for (final String node : nodesWithFailedShard) {
          _zkClient.delete(_conf.getZKShardToErrorPath(shard, node));
          _zkClient.deleteIfExists(_conf.getZKNodeToShardPath(node, shard));
        }
      }
    }

    // add shards to zk
    for (final String shard : indexShards) {
      final String shardZkPath = _conf.getZKShardPath(index, shard);
      final String shard2NodeRootPath = _conf.getZKShardToNodePath(shard);
      final String shard2ErrorRootPath = _conf.getZKShardToErrorPath(shard);
      if (!_zkClient.exists(shardZkPath)) {
        _zkClient.create(shardZkPath, shard2AssignedShardMap.get(shard));
      }
      if (!_zkClient.exists(shard2NodeRootPath)) {
        _zkClient.create(shard2NodeRootPath);
      }
      if (!_zkClient.exists(shard2ErrorRootPath)) {
        _zkClient.create(shard2ErrorRootPath);
      }
    }

    // now distribute shards
    final Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, indexShards);
    final Map<String, List<String>> currentNodeToShardsMap = readNode2ShardsMapFromZk(_zkClient);
    final Map<String, List<String>> distributionMap = _deployPolicy.createDistributionPlan(currentShard2NodesMap,
            currentNodeToShardsMap, new ArrayList<String>(liveNodes), indexMD.getReplicationLevel());
    writeShardDistributionMapToZK(distributionMap, shard2AssignedShardMap);

    final IndexStateListener indexStateListener = new IndexStateListener(index, indexMD, indexShards, liveNodes.size());
    _indexStateListeners.add(indexStateListener);
    indexStateListener.subscribeShardEvents();
  }

  private static Map<String, List<String>> readShard2NodesMapFromZk(final ZKClient zkClient,
          final Set<String> indexShards) throws KattaException {
    final Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (final String shard : indexShards) {
      final String shard2NodeRootPath = zkClient.getConfig().getZKShardToNodePath(shard);
      if (zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard, zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard, Collections.<String>emptyList());
      }
    }
    return shard2NodeNames;
  }

  private Map<String, List<String>> readNode2ShardsMapFromZk(final ZKClient zkClient) throws KattaException {
    final Map<String, List<String>> node2ShardNames = new HashMap<String, List<String>>();
    final List<String> nodes = zkClient.getChildren(_conf.getZKNodeToShardPath());
    for (final String node : nodes) {
      final String node2ShardRootPath = _conf.getZKNodeToShardPath(node);
      if (zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, Collections.<String>emptyList());
      }
    }
    return node2ShardNames;
  }

  private void writeShardDistributionMapToZK(final Map<String, List<String>> distributionMap,
          final Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      final List<String> existingShards = _zkClient.getChildren(_conf.getZKNodeToShardPath(node));
      final List<String> newShards = distributionMap.get(node);

      // add new shards
      for (final String shard2Deploy : CollectionUtil.getListOfAdded(existingShards, newShards)) {
        final String shard2NodePath = _conf.getZKNodeToShardPath(node, shard2Deploy);
        _zkClient.create(shard2NodePath, shard2AssignedShardMap.get(shard2Deploy));
      }

      // remove old shards
      for (final String shard2Deploy : CollectionUtil.getListOfRemoved(existingShards, newShards)) {
        _zkClient.delete(_conf.getZKNodeToShardPath(node, shard2Deploy));
      }
    }
  }

  private Map<String, AssignedShard> readShardsFromFs(final String index, final IndexMetaData indexMetaData)
          throws IndexInvalidException {
    final String indexPathString = indexMetaData.getPath();
    // get shard folders from source
    URI uri;
    try {
      uri = new URI(indexPathString);
    } catch (final URISyntaxException e) {
      throw new IndexInvalidException("unable to parse index path uri '" + indexPathString
              + "', make sure it starts with file:// or hdfs:// ", e);
    }
    FileSystem fileSystem;
    try {
      fileSystem = FileSystem.get(uri, new Configuration());
    } catch (final IOException e) {
      throw new IndexInvalidException("unable to retrive file system for index path '" + indexPathString
              + "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
    }
    final Map<String, AssignedShard> shard2AssignedShard = new HashMap<String, AssignedShard>();
    try {
      final Path indexPath = new Path(indexPathString);
      if (!fileSystem.exists(indexPath)) {
        throw new IndexInvalidException("index path '" + uri + "' does not exists");
      }
      final FileStatus[] listStatus = fileSystem.listStatus(indexPath, new PathFilter() {
        public boolean accept(final Path aPath) {
          return !aPath.getName().startsWith(".");
        }
      });
      for (final FileStatus fileStatus : listStatus) {
        if (fileStatus.isDir() || fileStatus.getPath().toString().endsWith(".zip")) {
          final AssignedShard assignedShard = new AssignedShard(index, fileStatus.getPath().toString());
          shard2AssignedShard.put(assignedShard.getShardName(), assignedShard);
        }
      }
    } catch (final IOException e) {
      throw new IndexInvalidException("could not access index path: " + indexPathString, e);
    }

    if (shard2AssignedShard.size() == 0) {
      throw new IndexInvalidException("index does not contain any shard");
    }
    return shard2AssignedShard;
  }

  protected static class StatusUpdate {

    private long _lastUpdate;

    public StatusUpdate() {
      _lastUpdate = System.currentTimeMillis();
    }

    public long lastChangeTimeStamp() {
      return _lastUpdate;
    }

    private final Set<String> _indexes = new HashSet<String>();
    private final Set<String> _nodes = new HashSet<String>();

    public void updateIndexes(final Collection<String> indexes) {
      _indexes.clear();
      _indexes.addAll(indexes);
      _lastUpdate = System.currentTimeMillis();
    }

    public void updateNodes(final Collection<String> nodes) {
      _nodes.clear();
      _nodes.addAll(nodes);
      _lastUpdate = System.currentTimeMillis();
    }

    public Set<String> getIndexes() {
      return _indexes;
    }

    public Set<String> getNodes() {
      return _nodes;
    }

    public boolean hasChanges(final Set<String> oldIndexes, final Set<String> oldNodes) {
      return !_indexes.equals(oldIndexes) || !_nodes.equals(oldNodes);
    }

  }

  protected class IndexStateListener implements IZkChildListener {

    private final Set<String> _shards;
    private final Map<String, Integer> _shardToReplicaCount = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Integer> _shardToErrorCount = new ConcurrentHashMap<String, Integer>();
    private final String _index;
    private final IndexMetaData _indexMetaData;
    private final int _replicationLevel;

    public IndexStateListener(final String index, final IndexMetaData indexMetaData, final Set<String> shards,
            final int nodeCount) {
      _index = index;
      _indexMetaData = indexMetaData;
      _shards = shards;

      // TODO jz: this should be part of the distributionPlan
      _replicationLevel = Math.min(_indexMetaData.getReplicationLevel(), nodeCount);
    }

    public void subscribeShardEvents() throws KattaException {
      _zkClient.getEventLock().lock();
      try {
        LOG.info("start watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
        final Set<String> shards = _shards;
        for (final String shard : shards) {
          final String shard2NodeRootPath = _conf.getZKShardToNodePath(shard);
          final String shard2ErrorPath = _conf.getZKShardToErrorPath(shard);
          _shardToReplicaCount.put(shard, _zkClient.subscribeChildChanges(shard2NodeRootPath, this).size());
          _shardToErrorCount.put(shard, _zkClient.subscribeChildChanges(shard2ErrorPath, this).size());
        }
        checkForIndexStateSwitch();
      } finally {
        _zkClient.getEventLock().unlock();
      }
    }

    public void unsubscribeShardEvents() {
      LOG.info("stop watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      final Set<String> shards = _shards;
      for (final String shard : shards) {
        final String shard2NodeRootPath = _conf.getZKShardToNodePath(shard);
        final String shard2ErrorPath = _conf.getZKShardToErrorPath(shard);
        _zkClient.unsubscribeChildChanges(shard2NodeRootPath, this);
        _zkClient.unsubscribeChildChanges(shard2ErrorPath, this);
      }
      _indexStateListeners.remove(this);
    }

    public void handleChildChange(final String parentPath, final List<String> currentChilds) throws KattaException {
      final String shard = _conf.getZKName(parentPath);
      if (parentPath.startsWith(_conf.getZKShardToNodePath())) {
        _shardToReplicaCount.put(shard, currentChilds.size());
      } else if (parentPath.startsWith(_conf.getZKShardToErrorPath())) {
        _shardToErrorCount.put(shard, currentChilds.size());
      } else {
        throw new IllegalStateException("could not associate path " + parentPath);
      }
      checkForIndexStateSwitch();
    }

    private synchronized void checkForIndexStateSwitch() throws KattaException {
      if (_indexMetaData.getState() == IndexState.DEPLOYED || _indexMetaData.getState() == IndexState.ERROR) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("check with following deployments:" + _shardToReplicaCount);
        LOG.debug("check with following errors:" + _shardToErrorCount);
      }
      int notDeployed = 0;
      int underReplicated = 0;
      int failed = 0;
      int failedCompletely = 0;

      for (final String shard : _shards) {
        final Integer replicaCount = _shardToReplicaCount.get(shard);
        if (replicaCount == null || replicaCount == 0) {
          notDeployed++;
        } else if (replicaCount < _replicationLevel) {
          underReplicated++;
        }
        final Integer errorCount = _shardToErrorCount.get(shard);
        if (errorCount != null) {
          failed += errorCount;
          if (errorCount >= _replicationLevel) {
            failedCompletely++;
          }
        }
      }

      if (failedCompletely > 0) {
        // at least one shard could not be deployed on any node
        LOG.info("index '" + _index + "' deployment failed");
        switchIndexState(IndexState.ERROR);
        unsubscribeShardEvents();
      } else if (notDeployed == 0 && underReplicated == 0 && failed == 0) {
        // all shards are fully replicated
        switchIndexState(IndexState.DEPLOYED);
        unsubscribeShardEvents();
      } else if (notDeployed == 0 && underReplicated > 0) {
        // all shards are at least 1 time deployed
        switchIndexState(IndexState.REPLICATING);
      } else if (notDeployed == 0 && underReplicated - failed == 0) {
        LOG.info("index '" + _index + "' deployed with errors");
        switchIndexState(IndexState.DEPLOYED);
        unsubscribeShardEvents();

        try {
          _updateLock.lock();
          // we wakeup the manager thread since part of this index needs to be
          // redeployed
          _updateLock.getUpdatedCondition().signalAll();
          // TODO sg: if a shard is corrupted it would fail to deploy on all
          // nodes. So in case a shard just failed once on one specific node,
          // than we can assume that with the next distribution map it will be
          // deployed on a different node. In order to allow distribution
          // policies to handle such cases we should pass in node-shard-error
          // map as well
        } finally {
          _updateLock.unlock();
        }
      }

    }

    private void switchIndexState(final IndexState indexState) throws KattaException {
      if (_indexMetaData.getState() == indexState) {
        return;
      }

      LOG
              .info("switching index '" + _index + "' from state " + _indexMetaData.getState() + " into state "
                      + indexState);
      final String indexZkPath = _conf.getZKIndexPath(_index);
      _zkClient.readData(indexZkPath, _indexMetaData);
      if (indexState == IndexState.ERROR) {
        _indexMetaData.setState(indexState, "could not deploy shards properly, please see node logs");
      } else {
        _indexMetaData.setState(indexState);
      }
      _zkClient.writeData(indexZkPath, _indexMetaData);
    }
  }

  private class IndexInvalidException extends Exception {

    private static final long serialVersionUID = 1L;

    public IndexInvalidException(final String message) {
      super(message);
    }

    public IndexInvalidException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }

  protected static class UpdateLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;
    private final Condition _updatedCondition = newCondition();

    // @Override
    // public void lock() {
    // new IOException(">>>>>>>>>>>>>>>>>>>>lock").printStackTrace();
    // super.lock();
    // }
    //    
    // @Override
    // public void unlock() {
    // new IOException("<<<<<<<<<<<<<<<<<<<<<<<unlock").printStackTrace();
    // super.unlock();
    // }

    /**
     * This condition will be signaled if a {@link StatusUpdate} has been
     * modified.
     */
    public Condition getUpdatedCondition() {
      return _updatedCondition;
    }

    public Collection<Thread> getThreads() {
      return getQueuedThreads();
    }

  }

}
