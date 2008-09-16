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

  private StatusUpdate _statusUpdate = new StatusUpdate();
  private UpdateLock _updateLock = new UpdateLock();

  public DistributeShardsThread(ZKClient zkClient, IDeployPolicy deployPolicy) {
    _deployPolicy = deployPolicy;
    _zkClient = zkClient;
    setDaemon(true);
    setName(getClass().getSimpleName());
  }

  public void reportStartup() {
    _updateLock.lock();
    _statusUpdate.setStartupReported(true);
    _updateLock.getUpdatedCondition().signal();
    _updateLock.unlock();
  }

  public void updateIndexes(Collection<String> indexes) {
    _updateLock.lock();
    _statusUpdate.updateIndexes(indexes);
    _updateLock.getUpdatedCondition().signal();
    _updateLock.unlock();
  }

  public void updateNodes(Collection<String> nodes) {
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
      while (true) {
        _updateLock.lock();
        if (!_statusUpdate.hasChanges(_liveIndexes, _liveNodes)) {
          _updateLock.getUpdatedCondition().await();
          // TODO jz: wait x ms and if nothing happens rebalance
        }
        LOG.info("processing of update started...");

        boolean startupReported = _statusUpdate.isStartupReported();
        Set<String> updatedIndexes = _statusUpdate.getIndexes();
        Set<String> updatedNodes = _statusUpdate.getNodes();
        if (updatedNodes.isEmpty()) {
          // jz: if connected nodes in under a certain threshold go in
          // safe-mode?
          LOG.warn("no nodes connected - delaying update");
          _updateLock.getUpdatedCondition().await();
          _updateLock.unlock();
          continue;
        }
        _statusUpdate.reset();
        _updateLock.unlock();

        try {
          // now do the work
          if (startupReported) {
            _liveIndexes = updatedIndexes;
            _liveNodes = updatedNodes;
            handleStartup();
          } else {
            Set<String> addedIndexes = CollectionUtil.getSetOfAdded(_liveIndexes, updatedIndexes);
            Set<String> removedIndexes = CollectionUtil.getSetOfRemoved(_liveIndexes, updatedIndexes);
            Set<String> addedNodes = CollectionUtil.getSetOfAdded(_liveNodes, updatedNodes);
            Set<String> removedNodes = CollectionUtil.getSetOfRemoved(_liveNodes, updatedNodes);

            _liveIndexes = updatedIndexes;
            _liveNodes = updatedNodes;
            handleRemovedIndexes(removedIndexes);// first free up space
            handleRemovedNodes(removedNodes);// "save" existing indexes
            handleAddedOrUnderreplicatedIndexes(addedIndexes);// do the work
            handleAddedNodes(addedNodes);// maybe rebalance
          }
        } catch (KattaException e) {
          if (e.getCause() instanceof InterruptedException) {
            throw (InterruptedException) e.getCause();
          }
          LOG.error("Failed to execute shard update to {" + toString(updatedIndexes, updatedNodes, startupReported)
              + "}", e);
        }
        LOG.info("processing of update finsihed!");
      }
    } catch (InterruptedException e) {
      LOG.info("manage shard thread stopped");
      try {
        _updateLock.unlock();
      } catch (Exception e2) {
        // ignore
      }
    }
  }

  private String toString(Set<String> updatedIndexes, Set<String> updatedNodes, boolean startupReported) {
    return "indexes: " + updatedIndexes + " | nodes: " + updatedNodes + " | startup: " + startupReported;
  }

  private void handleStartup() throws KattaException, InterruptedException {
    LOG.info("do integrity check of indexes");
    Set<String> underreplicatedIndexes = getUnderreplicatedIndexes();
    LOG.info("found following underreplicated indexes: " + underreplicatedIndexes);
    Set<String> overreplicatedIndexes = getOverreplicatedIndexes();
    LOG.info("found following overreplicated indexes: " + overreplicatedIndexes);
    Set<String> annoucedIndexes = getAnnouncedButUndeployedIndexes();
    LOG.info("found following indexes in announced state: " + annoucedIndexes);

    // now redeploy/replicate
    underreplicatedIndexes.addAll(overreplicatedIndexes);
    underreplicatedIndexes.addAll(annoucedIndexes);
    handleAddedOrUnderreplicatedIndexes(underreplicatedIndexes);

    // TODO jz: check namespace structure ??
    final List<String> nodes = _zkClient.getKnownNodes();
    for (final String node : nodes) {
      if (!_zkClient.exists(ZkPathes.getNodePath(node))) {
        LOG.info("clean up node without metadata " + node);
        _zkClient.deleteRecursive(ZkPathes.getNode2ShardRootPath(node));
      }
    }
  }

  private Set<String> getAnnouncedButUndeployedIndexes() throws KattaException {
    Set<String> announcedIndexes = new HashSet<String>();
    for (String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
      if (indexMetaData.getState() == IndexState.ANNOUNCED) {
        announcedIndexes.add(index);
      }
    }
    return announcedIndexes;
  }

  private Set<String> getUnderreplicatedIndexes() throws KattaException {
    Set<String> underreplicatedIndexes = new HashSet<String>();
    for (String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      String indexZkPath = ZkPathes.getIndexPath(index);
      IndexMetaData indexMetaData = new IndexMetaData();
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
    Set<String> overreplicatedIndexes = new HashSet<String>();
    for (String index : _zkClient.getChildren(ZkPathes.INDEXES)) {
      String indexZkPath = ZkPathes.getIndexPath(index);
      IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, indexMetaData);
      if (indexMetaData.getState() != IndexState.ERROR) {
        if (isOverReplicated(indexZkPath, indexMetaData)) {
          overreplicatedIndexes.add(index);
        }
      }
    }
    return overreplicatedIndexes;
  }

  private boolean isUnderReplicated(String indexZkPath, IndexMetaData indexMetaData) throws KattaException {
    List<String> shards = _zkClient.getChildren(indexZkPath);
    Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, new HashSet<String>(shards));
    for (String shard : shards) {
      int servingNodes = currentShard2NodesMap.get(shard).size();
      if (servingNodes < indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
  }

  private boolean isOverReplicated(String indexZkPath, IndexMetaData indexMetaData) throws KattaException {
    List<String> shards = _zkClient.getChildren(indexZkPath);
    Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, new HashSet<String>(shards));
    for (String shard : shards) {
      int servingNodes = currentShard2NodesMap.get(shard).size();
      if (servingNodes > indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
  }

  private void handleRemovedIndexes(Set<String> removedIndexes) throws KattaException {
    if (removedIndexes.isEmpty()) {
      return;
    }
    LOG.info("remove indexes: " + removedIndexes);
    // TODO: jz what if index is in replicate state ?

    // iterates through all nodes and removes the assigned shards from index
    for (String indexName : removedIndexes) {
      List<String> nodes = _zkClient.getKnownNodes();
      for (String node : nodes) {
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

  private void handleRemovedNodes(Set<String> removedNodes) throws KattaException, InterruptedException {
    if (removedNodes.isEmpty()) {
      return;
    }
    LOG.info("remove nodes: " + removedNodes);

    Set<String> affectedIndexes = new HashSet<String>();
    for (String node : removedNodes) {
      String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      List<String> shards = _zkClient.getChildren(node2ShardRootPath);
      for (String shard : shards) {
        AssignedShard assignedShard = new AssignedShard();
        _zkClient.readData(ZkPathes.getNode2ShardPath(node, shard), assignedShard);
        affectedIndexes.add(assignedShard.getIndexName());
      }
    }
    distributeShards(affectedIndexes, IndexState.REPLICATING);
  }

  private void handleAddedOrUnderreplicatedIndexes(Set<String> addedIndexes) throws KattaException,
      InterruptedException {
    if (addedIndexes.isEmpty()) {
      return;
    }
    LOG.info("distribute/replicate indexes: " + addedIndexes);
    distributeShards(addedIndexes, IndexState.DEPLOYING);
  }

  private void handleAddedNodes(Set<String> addedNodes) throws KattaException, InterruptedException {
    if (addedNodes.isEmpty()) {
      return;
    }
    LOG.info("add nodes: " + addedNodes);

    handleAddedOrUnderreplicatedIndexes(getUnderreplicatedIndexes());
    // TODO jz: rebalance nodes load ?
  }

  private void distributeShards(Set<String> affectedIndexes, IndexState state) throws KattaException,
      InterruptedException {
    for (String index : affectedIndexes) {
      final String indexZkPath = ZkPathes.getIndexPath(index);
      final IndexMetaData indexMetaData = new IndexMetaData();
      try {
        _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
        LOG.info(state.name().toLowerCase() + " shards for index '" + index + "' (" + indexMetaData.getState() + ")");

        Map<String, AssignedShard> shard2AssignedShardMap = readShardsFromFs(index, indexMetaData);
        Set<String> indexShards = shard2AssignedShardMap.keySet();
        LOG.info("Found shards '" + indexShards + "' for index '" + index + "'");

        indexMetaData.setState(state);
        _zkClient.writeData(indexZkPath, indexMetaData);
        distributeShards(index, indexZkPath, indexMetaData, indexShards, shard2AssignedShardMap);
      } catch (Exception e) {
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

  private void distributeShards(String index, String indexZkPath, IndexMetaData indexMD, Set<String> indexShards,
      Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {

    // cleanup/undeploy failed shards
    for (final String shard : indexShards) {
      final String shard2ErrorRootPath = ZkPathes.getShard2ErrorRootPath(shard);
      if (_zkClient.exists(shard2ErrorRootPath)) {
        List<String> nodesWithFailedShard = _zkClient.getChildren(shard2ErrorRootPath);
        for (String node : nodesWithFailedShard) {
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
    Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, indexShards);
    Map<String, List<String>> currentNodeToShardsMap = readNode2ShardsMapFromZk(_zkClient);
    final Map<String, List<String>> distributionMap = _deployPolicy.createDistributionPlan(currentShard2NodesMap,
        currentNodeToShardsMap, new ArrayList<String>(_liveNodes), indexMD.getReplicationLevel());
    writeShardDistributionMapToZK(distributionMap, shard2AssignedShardMap);
    indexMD.setState(IndexState.DEPLOYING);
    _zkClient.writeData(indexZkPath, indexMD);

    IndexStateListener indexStateListener = new IndexStateListener(_zkClient, index, indexMD, indexShards, _liveNodes
        .size());
    _zkClient.getEventLock().lock();
    indexStateListener.subscribeShardEvents();
    _zkClient.getEventLock().unlock();
  }

  private static Map<String, List<String>> readShard2NodesMapFromZk(ZKClient zkClient, Set<String> indexShards)
      throws KattaException {
    Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (String shard : indexShards) {
      String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
      if (zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard, zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard, Collections.EMPTY_LIST);
      }
    }
    return shard2NodeNames;
  }

  private Map<String, List<String>> readNode2ShardsMapFromZk(ZKClient zkClient) throws KattaException {
    Map<String, List<String>> node2ShardNames = new HashMap<String, List<String>>();
    List<String> nodes = zkClient.getChildren(ZkPathes.NODE_TO_SHARD);
    for (String node : nodes) {
      String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      if (zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, Collections.EMPTY_LIST);
      }
    }
    return node2ShardNames;
  }

  private void writeShardDistributionMapToZK(final Map<String, List<String>> distributionMap,
      Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      List<String> existingShards = _zkClient.getChildren(ZkPathes.getNode2ShardRootPath(node));
      final List<String> newShards = distributionMap.get(node);

      // add new shards
      for (String shard2Deploy : CollectionUtil.getListOfAdded(existingShards, newShards)) {
        final String shard2NodePath = ZkPathes.getNode2ShardPath(node, shard2Deploy);
        _zkClient.create(shard2NodePath, shard2AssignedShardMap.get(shard2Deploy));
      }

      // remove old shards
      for (String shard2Deploy : CollectionUtil.getListOfRemoved(existingShards, newShards)) {
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
    Map<String, AssignedShard> shard2AssignedShard = new HashMap<String, AssignedShard>();
    try {
      Path indexPath = new Path(indexPathString);
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
          AssignedShard assignedShard = new AssignedShard(index, fileStatus.getPath().toString());
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

    private boolean _startupReported;

    private Set<String> _indexes = new HashSet<String>();
    private Set<String> _nodes = new HashSet<String>();

    public boolean isStartupReported() {
      return _startupReported;
    }

    public void setStartupReported(boolean startupReported) {
      _startupReported = startupReported;
    }

    public void updateIndexes(Collection<String> indexes) {
      _indexes.clear();
      _indexes.addAll(indexes);
    }

    public void updateNodes(Collection<String> nodes) {
      _nodes.clear();
      _nodes.addAll(nodes);
    }

    public Set<String> getIndexes() {
      if (_indexes.isEmpty()) {
        return Collections.EMPTY_SET;
      }
      return new HashSet<String>(_indexes);
    }

    public Set<String> getNodes() {
      if (_nodes.isEmpty()) {
        return Collections.EMPTY_SET;
      }
      return new HashSet<String>(_nodes);
    }

    public boolean hasChanges(Set<String> oldIndexes, Set<String> oldNodes) {
      return _startupReported || !_indexes.equals(oldIndexes) || !_nodes.equals(oldNodes);
    }

    public void reset() {
      _startupReported = false;
    }
  }

  private static class IndexStateListener implements IZkChildListener {

    private final Set<String> _shards;
    private final ZKClient _zkClient;
    private final Map<String, Integer> _shardToReplicaCount = new HashMap<String, Integer>();
    private final Map<String, Integer> _shardToErrorCount = new HashMap<String, Integer>();
    private final String _index;
    private final IndexMetaData _indexMetaData;
    private int _replicationLevel;

    public IndexStateListener(ZKClient zkClient, String index, IndexMetaData indexMetaData, Set<String> shards,
        int nodeCount) {
      _index = index;
      _indexMetaData = indexMetaData;
      _shards = shards;
      _zkClient = zkClient;

      // TODO jz: this should be part of the distributionPlan
      _replicationLevel = Math.min(_indexMetaData.getReplicationLevel(), nodeCount);
    }

    public void subscribeShardEvents() throws KattaException {
      LOG.info("start watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      Set<String> shards = _shards;
      for (String shard : shards) {
        String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
        String shard2ErrorPath = ZkPathes.getShard2ErrorRootPath(shard);
        _shardToReplicaCount.put(shard, _zkClient.subscribeChildChanges(shard2NodeRootPath, this).size());
        _shardToErrorCount.put(shard, _zkClient.subscribeChildChanges(shard2ErrorPath, this).size());
      }
      checkForIndexStateSwitch();
    }

    private void unsubscribeShardEvents() {
      LOG.info("stop watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      Set<String> shards = _shards;
      for (String shard : shards) {
        String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
        String shard2ErrorPath = ZkPathes.getShard2ErrorRootPath(shard);
        _zkClient.unsubscribeChildChanges(shard2NodeRootPath, this);
        _zkClient.unsubscribeChildChanges(shard2ErrorPath, this);
      }
    }

    public void handleChildChange(String parentPath, List<String> currentChilds) throws KattaException {
      String shard = ZkPathes.getName(parentPath);
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
        return;
      }
      int notDeployed = 0;
      int underReplicated = 0;
      int failed = 0;
      int failedCompletely = 0;

      for (String shard : _shards) {
        Integer replicaCount = _shardToReplicaCount.get(shard);
        if (replicaCount == null || replicaCount == 0) {
          notDeployed++;
        } else if (replicaCount < _replicationLevel) {
          underReplicated++;
        }
        Integer errorCount = _shardToErrorCount.get(shard);
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

    private void switchIndexState(IndexState indexState) throws KattaException {
      if (_indexMetaData.getState() == indexState) {
        return;
      }

      LOG
          .info("switching index '" + _index + "' from state " + _indexMetaData.getState() + " into state "
              + indexState);
      String indexZkPath = ZkPathes.getIndexPath(_index);
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

    public IndexInvalidException(String message) {
      super(message);
    }

    public IndexInvalidException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  protected static class UpdateLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;
    private Condition _updatedCondition = newCondition();

    /**
     * This condition will be signaled if a {@link StatusUpdate} has been
     * modified.
     */
    public Condition getUpdatedCondition() {
      return _updatedCondition;
    }

  }

}
