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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class DistributeShardsThread extends Thread {

  protected final static Logger LOG = Logger.getLogger(DistributeShardsThread.class);

  private final InteractionProtocol _interactionProtocol;
  private final IDeployPolicy _deployPolicy;
  private final long _safeModeMaxTime;

  private final StatusUpdate _statusUpdate = new StatusUpdate();
  private final UpdateLock _updateLock = new UpdateLock();

  private boolean _safeMode;

  public DistributeShardsThread(InteractionProtocol interactionProtocol, final IDeployPolicy deployPolicy,
          final long safeModeMaxTime) {
    _interactionProtocol = interactionProtocol;
    setDaemon(true);
    setName(getClass().getSimpleName());
    _deployPolicy = deployPolicy;
    _safeModeMaxTime = safeModeMaxTime;
  }

  public void addIndex(String node) {
    _statusUpdate.addIndex(node);
    signalUpdate();
  }

  public void removeIndex(String node) {
    _statusUpdate.removeIndex(node);
    signalUpdate();
  }

  public void addNode(String node) {
    _statusUpdate.addNode(node);
    signalUpdate();
  }

  public void removeNode(String node) {
    _statusUpdate.removeNode(node);
    signalUpdate();
  }

  private void signalUpdate() {
    try {
      _updateLock.lock();
      _updateLock.getUpdatedCondition().signal();
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
      LOG.info("manage shard thread stopped");
    }
  }

  private void runInSafeMode(Set<String> liveNodes, Set<String> liveIndexes) throws InterruptedException {
    _safeMode = true;
    try {
      while (_statusUpdate.lastChangeTimeStamp() + _safeModeMaxTime > System.currentTimeMillis()
              || _statusUpdate.getNodes().isEmpty() || areNodesConnecting(_statusUpdate.getNodes())) {
        LOG.info("SAFE MODE: No nodes available or state unstable within the last " + _safeModeMaxTime + " ms.");
        _updateLock.lock();
        try {
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
      try {
        NodeMetaData nodeMetaData = _interactionProtocol.getNodeMD(node);
        if (nodeMetaData.getState() == NodeState.STARTING || nodeMetaData.getState() == NodeState.RECONNECTING) {
          return true;
        }
      } catch (ZkException e) {
        // TODO jz: no zk stuff here
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

  private Set<String> getIndexesInState(IndexState indexState) {
    final Set<String> indexes = new HashSet<String>();
    for (final String index : _interactionProtocol.getIndices()) {
      final IndexMetaData indexMetaData = _interactionProtocol.getIndexMD(index);
      if (indexMetaData.getState() == indexState) {
        indexes.add(index);
      }
    }
    if (!indexes.isEmpty()) {
      LOG.info("found following indexes in state '" + indexState + "': " + indexes);
    }
    return indexes;
  }

  private Set<String> getUnbalancedIndexes(int nodeCount) {
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

  private Set<String> getUnderreplicatedIndexes(int nodeCount) {
    final Set<String> underreplicatedIndexes = new HashSet<String>();
    for (final String index : _interactionProtocol.getIndices()) {
      final IndexMetaData indexMetaData = _interactionProtocol.getIndexMD(index);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        int desiredReplicationCount = indexMetaData.getReplicationLevel();
        int minimalReplicationCount = getMinimalReplicationCount(index, desiredReplicationCount);
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

  private Set<String> getOverreplicatedIndexes() {
    final Set<String> overreplicatedIndexes = new HashSet<String>();
    for (final String index : _interactionProtocol.getIndices()) {
      final IndexMetaData indexMetaData = _interactionProtocol.getIndexMD(index);
      if (indexMetaData.getState() != IndexState.ERROR && indexMetaData.getState() != IndexState.DEPLOYING
              && indexMetaData.getState() != IndexState.REPLICATING) {
        if (isOverReplicated(index, indexMetaData)) {
          overreplicatedIndexes.add(index);
        }
      }
    }
    return overreplicatedIndexes;
  }

  private int getMinimalReplicationCount(final String index, int desiredReplicationCount) {
    int minimalReplicationCount = desiredReplicationCount;
    final List<String> shards = _interactionProtocol.getIndexShards(index);
    final Map<String, List<String>> shard2NodesMap = _interactionProtocol.getShard2NodesMap(shards);
    for (final String shard : shards) {
      final int servingNodes = shard2NodesMap.get(shard).size();
      if (servingNodes < minimalReplicationCount) {
        minimalReplicationCount = servingNodes;
      }
    }
    return minimalReplicationCount;
  }

  private boolean isOverReplicated(final String index, final IndexMetaData indexMetaData) {
    final List<String> shards = _interactionProtocol.getIndexShards(index);
    final Map<String, List<String>> currentShard2NodesMap = _interactionProtocol.getShard2NodesMap(shards);
    for (final String shard : shards) {
      final int servingNodes = currentShard2NodesMap.get(shard).size();
      if (servingNodes > indexMetaData.getReplicationLevel()) {
        return true;
      }
    }
    return false;
  }

  private void handleRemovedIndexes(final Set<String> removedIndexes) {
    if (removedIndexes.isEmpty()) {
      return;
    }
    LOG.info("remove indexes: " + removedIndexes);
    // TODO: jz what if index is in replicate state ?

    // iterates through all nodes and removes the assigned shards from index
    for (final String indexName : removedIndexes) {
      _interactionProtocol.undeployIndex(indexName);
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
      // final String indexZkPath = _conf.getZKIndexPath(index);
      try {
        IndexMetaData indexMetaData = _interactionProtocol.getIndexMD(index);
        LOG.info(state.name().toLowerCase() + " shards for index '" + index + "' (" + indexMetaData.getState() + ")");

        final Map<String, AssignedShard> shard2AssignedShardMap = readShardsFromFs(index, indexMetaData);
        final Set<String> indexShards = shard2AssignedShardMap.keySet();
        LOG.info("Found shards '" + indexShards + "' for index '" + index + "'");

        indexMetaData.setState(state);
        _interactionProtocol.updateIndexMD(indexMetaData);
        distributeIndexShards(index, indexMetaData, indexShards, shard2AssignedShardMap, liveNodes);
      } catch (final Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          throw new KattaException("Distribution of shards was interrupted", e.getCause());
        }
        LOG.error("could not deploy index '" + index + "'", e);
        IndexMetaData indexMetaData = _interactionProtocol.getIndexMD(index);
        indexMetaData.setState(IndexState.ERROR, StringUtils.stringifyException(e));
        _interactionProtocol.updateIndexMD(indexMetaData);
      }
    }
  }

  private void distributeIndexShards(final String index, final IndexMetaData indexMD, final Set<String> shards,
          final Map<String, AssignedShard> shard2AssignedShardMap, Collection<String> liveNodes) {
    // cleanup/undeploy failed shards
    _interactionProtocol.cleanupShardData(shards);

    // add shards to zk
    _interactionProtocol.createShardData(index, shards, shard2AssignedShardMap);

    // now distribute shards
    final Map<String, List<String>> currentShard2NodesMap = _interactionProtocol.getShard2NodesMap(shards);
    final Map<String, List<String>> currentNodeToShardsMap = _interactionProtocol.getNode2ShardsMap();
    final Map<String, List<String>> distributionMap = _deployPolicy.createDistributionPlan(currentShard2NodesMap,
            currentNodeToShardsMap, new ArrayList<String>(liveNodes), indexMD.getReplicationLevel());
    writeShardDistributionMapToZK(distributionMap, shard2AssignedShardMap);

    _interactionProtocol.keepDeployStateSynced(index, indexMD, shards, liveNodes.size());
  }

  private void writeShardDistributionMapToZK(final Map<String, List<String>> distributionMap,
          final Map<String, AssignedShard> shard2AssignedShardMap) {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      final List<String> newShards = distributionMap.get(node);
      _interactionProtocol.deployShards(node, newShards, shard2AssignedShardMap);
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

    public void addIndex(String name) {
      _indexes.add(name);
      _lastUpdate = System.currentTimeMillis();
    }

    public void removeIndex(String name) {
      _indexes.remove(name);
      _lastUpdate = System.currentTimeMillis();
    }

    public void updateIndexes(final Collection<String> indexes) {
      _indexes.clear();
      _indexes.addAll(indexes);
      _lastUpdate = System.currentTimeMillis();
    }

    public void addNode(String name) {
      _nodes.add(name);
      _lastUpdate = System.currentTimeMillis();
    }

    public void removeNode(String name) {
      _nodes.remove(name);
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

    public Collection<Thread> getThreads() {
      return getQueuedThreads();
    }

  }

}
