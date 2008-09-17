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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class DistributeShardsThread extends Thread {

  protected final static Logger LOG = Logger.getLogger(DistributeShardsThread.class);

  private final ZKClient _zkClient;
  private final IDeployPolicy _deployPolicy;

  private Set<String> _liveNodes = new HashSet<String>();
  private Set<String> _liveIndexes = new HashSet<String>();

  private final StatusUpdate _statusUpdate = new StatusUpdate();
  private final UpdateLock _updateLock = new UpdateLock();

  private final long _safeModeMaxTime;

  private final List<IndexStateListener> _indexStateListeners = new ArrayList<IndexStateListener>();

  public DistributeShardsThread(final ZKClient zkClient, final IDeployPolicy deployPolicy, final long safeModeMaxTime) {
    _deployPolicy = deployPolicy;
    _zkClient = zkClient;
    _safeModeMaxTime = safeModeMaxTime;
    setDaemon(true);
    setName(getClass().getSimpleName());
  }

  public void updateIndexes(final Collection<String> indexes) {
    _updateLock.lock();
    _statusUpdate.updateIndexes(indexes);
    _updateLock.getUpdatedCondition().signal();
    _updateLock.unlock();
  }

  public void updateNodes(final Collection<String> nodes) {
    _updateLock.lock();
    _statusUpdate.updateNodes(nodes);
    _updateLock.getUpdatedCondition().signal();
    _updateLock.unlock();
  }

  public Set<String> getLiveNodes() {
    return _liveNodes;
  }

  public Set<String> getLiveIndexes() {
    return _liveIndexes;
  }

  @Override
  public void run() {
    try {
      LOG.info("starting...");
      _updateLock.lock();
      try {
        while (_statusUpdate.lastChangeTimeStamp() + _safeModeMaxTime > System.currentTimeMillis()
            || _statusUpdate.getNodes().isEmpty()) {
          LOG.info("SAFE MODE: No nodes available or state unstable within the last " + _safeModeMaxTime + " ms.");
          _updateLock.getUpdatedCondition().await(_safeModeMaxTime, TimeUnit.MILLISECONDS);
        }
      } finally {
        _updateLock.unlock();
      }

      boolean startup = true;
      Set<String> updatedIndexes;
      Set<String> updatedNodes;
      while (true) {
        _updateLock.lock();
        try {
          while (!_statusUpdate.hasChanges(_liveIndexes, _liveNodes) || _statusUpdate.getNodes().isEmpty()) {
            if (_statusUpdate.getNodes().isEmpty()) {
              LOG.warn("no nodes connected - delaying update");
              // jz: if connected nodes in under a certain threshold go in
              // safe-mode?
            }
            _updateLock.getUpdatedCondition().await();
            // TODO jz: wait x ms and if nothing happens rebalance ??
          }
          LOG.info("processing of update started...");
          updatedIndexes = new HashSet<String>(_statusUpdate.getIndexes());
          updatedNodes = new HashSet<String>(_statusUpdate.getNodes());
        } finally {
          _updateLock.unlock();
        }

        try {
          // now do the work
          if (startup) {
            _liveIndexes = updatedIndexes;
            _liveNodes = updatedNodes;
            handleStartup();
            startup = false;
          } else {
            final Set<String> addedIndexes = CollectionUtil.getSetOfAdded(_liveIndexes, updatedIndexes);
            final Set<String> removedIndexes = CollectionUtil.getSetOfRemoved(_liveIndexes, updatedIndexes);
            final Set<String> addedNodes = CollectionUtil.getSetOfAdded(_liveNodes, updatedNodes);
            final Set<String> removedNodes = CollectionUtil.getSetOfRemoved(_liveNodes, updatedNodes);

            _liveIndexes = updatedIndexes;
            _liveNodes = updatedNodes;
            handleRemovedIndexes(removedIndexes);// first free up space
            handleRemovedNodes(removedNodes);// "save" existing indexes
            handleAddedOrUnderreplicatedIndexes(addedIndexes);// do the work
            handleAddedNodes(addedNodes);// maybe rebalance
          }
        } catch (final KattaException e) {
          if (e.getCause() instanceof InterruptedException) {
            throw (InterruptedException) e.getCause();
          }
          LOG.error("Failed to execute shard update to {" + toString(updatedIndexes, updatedNodes, startup) + "}", e);
        }
        LOG.info("processing of update finsihed!");
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

  private String toString(final Set<String> updatedIndexes, final Set<String> updatedNodes,
      final boolean startupReported) {
    return "indexes: " + updatedIndexes + " | nodes: " + updatedNodes + " | startup: " + startupReported;
  }

  private void handleStartup() throws KattaException, InterruptedException {
    LOG.info("do integrity check of indexes");
    final Set<String> underreplicatedIndexes = getUnderreplicatedIndexes();
    LOG.info("found following underreplicated indexes: " + underreplicatedIndexes);
    final Set<String> overreplicatedIndexes = getOverreplicatedIndexes();
    LOG.info("found following overreplicated indexes: " + overreplicatedIndexes);
    final Set<String> annoucedIndexes = getAnnouncedButUndeployedIndexes();
    LOG.info("found following indexes in announced state: " + annoucedIndexes);

    // now redeploy/replicate
    underreplicatedIndexes.addAll(overreplicatedIndexes);
    underreplicatedIndexes.addAll(annoucedIndexes);
    handleAddedOrUnderreplicatedIndexes(underreplicatedIndexes);

    // TODO jz: check namespace structure ??
  }

  private Set<String> getAnnouncedButUndeployedIndexes() throws KattaException {
    final Set<String> announcedIndexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
      if (indexMetaData.getState() == IndexState.ANNOUNCED) {
        announcedIndexes.add(index);
      }
    }
    return announcedIndexes;
  }

  private Set<String> getUnderreplicatedIndexes() throws KattaException {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      final String indexZkPath = ZkPathes.getIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, indexMetaData);
      if (indexMetaData.getState() != IndexState.ERROR) {
        if (isUnderReplicated(indexZkPath, indexMetaData)) {
          underreplicatedIndexes.add(index);
        }
      }
    }
    return underreplicatedIndexes;
  }

  private Set<String> getOverreplicatedIndexes() throws KattaException {
    final Set<String> overreplicatedIndexes = new HashSet<String>();
    for (final String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      final String indexZkPath = ZkPathes.getIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, indexMetaData);
      if (indexMetaData.getState() != IndexState.ERROR) {
        if (isOverReplicated(indexZkPath, indexMetaData)) {
          overreplicatedIndexes.add(index);
        }
      }
    }
    return overreplicatedIndexes;
  }

  private boolean isUnderReplicated(final String indexZkPath, final IndexMetaData indexMetaData) throws KattaException {
    final List<String> shards = _zkClient.getChildren(indexZkPath);
    final Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, new HashSet<String>(
        shards));
    for (final String shard : shards) {
      final int servingNodes = currentShard2NodesMap.get(shard).size();
      if (servingNodes < indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
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
        final List<String> shards = _zkClient.getChildren(ZkPathes.getNode2ShardRootPath(node));
        for (final String shard : shards) {
          final String node2ShardPath = ZkPathes.getNode2ShardPath(node, shard);
          final AssignedShard shardWritable = new AssignedShard();
          _zkClient.readData(node2ShardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _zkClient.delete(node2ShardPath);
          }
        }
      }
    }
  }

  private void handleRemovedNodes(final Set<String> removedNodes) throws KattaException, InterruptedException {
    if (removedNodes.isEmpty()) {
      return;
    }
    LOG.info("remove nodes: " + removedNodes);

    final Set<String> affectedIndexes = new HashSet<String>();
    for (final String node : removedNodes) {
      final String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      final List<String> shards = _zkClient.getChildren(node2ShardRootPath);
      for (final String shard : shards) {
        final AssignedShard assignedShard = new AssignedShard();
        _zkClient.readData(ZkPathes.getNode2ShardPath(node, shard), assignedShard);
        affectedIndexes.add(assignedShard.getIndexName());
      }
    }
    distributeShards(affectedIndexes, IndexState.REPLICATING);
  }

  private void handleAddedOrUnderreplicatedIndexes(final Set<String> addedIndexes) throws KattaException,
  InterruptedException {
    if (addedIndexes.isEmpty()) {
      return;
    }
    LOG.info("distribute/replicate indexes: " + addedIndexes);
    distributeShards(addedIndexes, IndexState.DEPLOYING);
  }

  private void handleAddedNodes(final Set<String> addedNodes) throws KattaException, InterruptedException {
    if (addedNodes.isEmpty()) {
      return;
    }
    LOG.info("add nodes: " + addedNodes);

    // most likely when a node comes back again we have over replication
    final Set<String> overreplicatedIndexes = getOverreplicatedIndexes();
    distributeShards(overreplicatedIndexes, IndexState.REPLICATING);

    // in case there wasn't enough nodes before to fit the replication level..
    final Set<String> underreplicatedIndexes = getUnderreplicatedIndexes();
    distributeShards(underreplicatedIndexes, IndexState.REPLICATING);

    // handleAddedOrUnderreplicatedIndexes(getUnderreplicatedIndexes());
    // TODO jz: rebalance nodes load ?
  }

  private void distributeShards(final Set<String> affectedIndexes, final IndexState state) throws KattaException,
  InterruptedException {
    for (final String index : affectedIndexes) {
      final String indexZkPath = ZkPathes.getIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      try {
        _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
        LOG.info(state.name().toLowerCase() + " shards for index '" + index + "' (" + indexMetaData.getState() + ")");

        final Map<String, AssignedShard> shard2AssignedShardMap = readShardsFromFs(index, indexMetaData);
        final Set<String> indexShards = shard2AssignedShardMap.keySet();
        LOG.info("Found shards '" + indexShards + "' for index '" + index + "'");

        indexMetaData.setState(state);
        _zkClient.writeData(indexZkPath, indexMetaData);
        distributeShards(index, indexZkPath, indexMetaData, indexShards, shard2AssignedShardMap);
      } catch (final Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          throw (InterruptedException) e.getCause();
        }
        LOG.error("could not deploy index '" + index + "'", e);
        _zkClient.readData(indexZkPath, indexMetaData);
        indexMetaData.setState(IndexState.ERROR, e.getMessage());
        _zkClient.writeData(indexZkPath, indexMetaData);
      }
    }
  }

  private void distributeShards(final String index, final String indexZkPath, final IndexMetaData indexMD,
      final Set<String> indexShards, final Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {

    // cleanup/undeploy failed shards
    for (final String shard : indexShards) {
      final String shard2ErrorRootPath = ZkPathes.getShard2ErrorRootPath(shard);
      if (_zkClient.exists(shard2ErrorRootPath)) {
        final List<String> nodesWithFailedShard = _zkClient.getChildren(shard2ErrorRootPath);
        for (final String node : nodesWithFailedShard) {
          _zkClient.delete(ZkPathes.getNode2ShardPath(node, shard));
          _zkClient.delete(ZkPathes.getShard2ErrorPath(shard, node));
        }
      }
    }

    // add shards to zk
    for (final String shard : indexShards) {
      final String shardZkPath = ZkPathes.getShardPath(index, shard);
      final String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
      final String shard2ErrorRootPath = ZkPathes.getShard2ErrorRootPath(shard);
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
        currentNodeToShardsMap, new ArrayList<String>(_liveNodes), indexMD.getReplicationLevel());
    writeShardDistributionMapToZK(distributionMap, shard2AssignedShardMap);
    // sg: we just wrote the index stage to something different like replication
    // and don't want to change it now to deploying.

    // indexMD.setState(IndexState.DEPLOYING);
    // _zkClient.writeData(indexZkPath, indexMD);

    final IndexStateListener indexStateListener = new IndexStateListener(_zkClient, index, indexMD, indexShards,
        _liveNodes.size());
    // sg: hold the listener so we can cleanly disconnect in case of a thread
    // shutdown
    _indexStateListeners.add(indexStateListener);
    _zkClient.getEventLock().lock();
    try {
      indexStateListener.subscribeShardEvents();
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  private static Map<String, List<String>> readShard2NodesMapFromZk(final ZKClient zkClient,
      final Set<String> indexShards) throws KattaException {
    final Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (final String shard : indexShards) {
      final String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
      if (zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard, zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard, Collections.EMPTY_LIST);
      }
    }
    return shard2NodeNames;
  }

  private Map<String, List<String>> readNode2ShardsMapFromZk(final ZKClient zkClient) throws KattaException {
    final Map<String, List<String>> node2ShardNames = new HashMap<String, List<String>>();
    final List<String> nodes = zkClient.getChildren(ZkPathes.NODE_TO_SHARD);
    for (final String node : nodes) {
      final String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      if (zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, Collections.EMPTY_LIST);
      }
    }
    return node2ShardNames;
  }

  private void writeShardDistributionMapToZK(final Map<String, List<String>> distributionMap,
      final Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      final List<String> existingShards = _zkClient.getChildren(ZkPathes.getNode2ShardRootPath(node));
      final List<String> newShards = distributionMap.get(node);

      // add new shards
      for (final String shard2Deploy : CollectionUtil.getListOfAdded(existingShards, newShards)) {
        final String shard2NodePath = ZkPathes.getNode2ShardPath(node, shard2Deploy);
        _zkClient.create(shard2NodePath, shard2AssignedShardMap.get(shard2Deploy));
      }

      // remove old shards
      for (final String shard2Deploy : CollectionUtil.getListOfRemoved(existingShards, newShards)) {
        _zkClient.delete(ZkPathes.getNode2ShardPath(node, shard2Deploy));
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
    private final ZKClient _zkClient;
    private final Map<String, Integer> _shardToReplicaCount = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Integer> _shardToErrorCount = new ConcurrentHashMap<String, Integer>();
    private final String _index;
    private final IndexMetaData _indexMetaData;
    private final int _replicationLevel;

    public IndexStateListener(final ZKClient zkClient, final String index, final IndexMetaData indexMetaData,
        final Set<String> shards, final int nodeCount) {
      _index = index;
      _indexMetaData = indexMetaData;
      _shards = shards;
      _zkClient = zkClient;

      // TODO jz: this should be part of the distributionPlan
      _replicationLevel = Math.min(_indexMetaData.getReplicationLevel(), nodeCount);
    }

    public void subscribeShardEvents() throws KattaException {
      LOG.info("start watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      final Set<String> shards = _shards;
      for (final String shard : shards) {
        final String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
        final String shard2ErrorPath = ZkPathes.getShard2ErrorRootPath(shard);
        _shardToReplicaCount.put(shard, _zkClient.subscribeChildChanges(shard2NodeRootPath, this).size());
        _shardToErrorCount.put(shard, _zkClient.subscribeChildChanges(shard2ErrorPath, this).size());
      }
      checkForIndexStateSwitch();
    }

    private void unsubscribeShardEvents() {
      LOG.info("stop watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      final Set<String> shards = _shards;
      for (final String shard : shards) {
        final String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
        final String shard2ErrorPath = ZkPathes.getShard2ErrorRootPath(shard);
        _zkClient.unsubscribeChildChanges(shard2NodeRootPath, this);
        _zkClient.unsubscribeChildChanges(shard2ErrorPath, this);
      }
      _indexStateListeners.remove(this);
    }

    public void handleChildChange(final String parentPath, final List<String> currentChilds) throws KattaException {
      final String shard = ZkPathes.getName(parentPath);
      if (parentPath.startsWith(ZkPathes.SHARD_TO_NODE)) {
        _shardToReplicaCount.put(shard, currentChilds.size());
      } else if (parentPath.startsWith(ZkPathes.SHARD_TO_ERROR)) {
        _shardToErrorCount.put(shard, currentChilds.size());
      } else {
        throw new IllegalStateException("could not associate path " + parentPath);
      }
      checkForIndexStateSwitch();
    }

    private synchronized void checkForIndexStateSwitch() throws KattaException {
      if (_indexMetaData.getState() == IndexState.DEPLOYED || _indexMetaData.getState() == IndexState.ERROR) {
        // sg: should'nt we unsubribe here too?
        unsubscribeShardEvents();
        return;
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
        // TODO jz: reschedule replication (but how avoid an endless loop)?
        unsubscribeShardEvents();
      }

    }

    private void switchIndexState(final IndexState indexState) throws KattaException {
      if (_indexMetaData.getState() == indexState) {
        return;
      }

      LOG
      .info("switching index '" + _index + "' from state " + _indexMetaData.getState() + " into state "
          + indexState);
      final String indexZkPath = ZkPathes.getIndexPath(_index);
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

    /**
     * This condition will be signaled if a {@link StatusUpdate} has been
     * modified.
     */
    public Condition getUpdatedCondition() {
      return _updatedCondition;
    }

  }

}
