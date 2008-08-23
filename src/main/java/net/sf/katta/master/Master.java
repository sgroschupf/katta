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
package net.sf.katta.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkDataListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.log4j.Logger;

public class Master {

  protected final static Logger LOG = Logger.getLogger(Master.class);

  protected DistributeShardsThread _distributeShardThread;
  protected ZKClient _zkClient;

  protected List<String> _nodes = new ArrayList<String>();
  protected List<String> _indexes = new ArrayList<String>();

  protected boolean _isMaster;

  public Master(final ZKClient zkClient) throws KattaException {
    _zkClient = zkClient;
    final MasterConfiguration masterConfiguration = new MasterConfiguration();
    final String deployPolicyClassName = masterConfiguration.getDeployPolicy();
    IDeployPolicy deployPolicy;
    try {
      final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicyClassName);
      deployPolicy = policyClazz.newInstance();
    } catch (final Exception e) {
      throw new KattaException("Unable to instantiate deploy policy", e);
    }
    _distributeShardThread = new DistributeShardsThread(_zkClient, deployPolicy);
  }

  public void start() throws KattaException {
    if (!_zkClient.isStarted()) {
      _zkClient.start(300000);
    }
    becomeMasterOrSecondaryMaster();
    if (_isMaster) {
      startNodeManagement();
      _distributeShardThread.start();
      startIndexManagement();
    }
  }

  public void shutdown() {
    _distributeShardThread.interrupt();
    try {
      _zkClient.unsubscribe();
      _zkClient.delete(ZkPathes.MASTER);
    } catch (KattaException e) {
      LOG.error("could bot delete the master data from zk");
    }
    _zkClient.close();
  }

  private void becomeMasterOrSecondaryMaster() throws KattaException {
    synchronized (_zkClient.getSyncMutex()) {
      final String hostName = NetworkUtil.getLocalhostName();
      cleanupOldMasterData(hostName);

      final MasterMetaData freshMaster = new MasterMetaData(hostName, System.currentTimeMillis());
      if (!_zkClient.exists(ZkPathes.MASTER)) {
        LOG.info(hostName + " starting as master...");
        _isMaster = true;
        _zkClient.createEphemeral(ZkPathes.MASTER, freshMaster);
      } else {
        LOG.info(hostName + " starting as secondary master...");
        _isMaster = false;
        _zkClient.subscribeDataChanges(ZkPathes.MASTER, new MasterListener());
      }
    }
  }

  private void cleanupOldMasterData(final String hostName) throws KattaException {
    if (_zkClient.exists(ZkPathes.MASTER)) {
      final MasterMetaData existingMaster = new MasterMetaData(hostName, System.currentTimeMillis());
      _zkClient.readData(ZkPathes.MASTER, existingMaster);
      if (existingMaster.getMasterName().equals(hostName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(ZkPathes.MASTER);
      }
    }
  }

  private void startIndexManagement() throws KattaException {
    LOG.debug("Loading indexes...");
    synchronized (_zkClient.getSyncMutex()) {
      _indexes = _zkClient.subscribeChildChanges(ZkPathes.INDEXES, new IndexListener());
      // TODO jz: do a integrety check ?
      for (String index : _indexes) {
        _distributeShardThread.addIndex(index);
      }
    }
  }

  private void startNodeManagement() throws KattaException {
    LOG.info("start managing nodes...");
    synchronized (_zkClient.getSyncMutex()) {
      _nodes = _zkClient.subscribeChildChanges(ZkPathes.NODES, new NodeListener());
      _distributeShardThread.updateNodes(_nodes);
    }
  }

  protected void removeNodes(final List<String> removedNodes) throws KattaException {
    for (final String node : removedNodes) {
      // get the shards this node served...
      final String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      final List<String> assignedShards = _zkClient.getChildren(node2ShardRootPath);
      final List<AssignedShard> shards = new ArrayList<AssignedShard>();
      for (final String shardName : assignedShards) {
        final AssignedShard metaData = new AssignedShard();
        _zkClient.readData(ZkPathes.getNode2ShardPath(node2ShardRootPath, shardName), metaData);
        shards.add(metaData);
      }
      _zkClient.deleteRecursive(node2ShardRootPath);
      // if (assignedShards.size() != 0) {
      // // since we lost one shard, we want to use replication level 1,
      // // since all other replica still exists..
      // List<String> nodes = readNodes();
      // if (nodes.size() > 0) {
      // final Map<String, List<AssignedShard>> asignmentMap =
      // _policy.distribute(_zkClient, nodes, shards, 1);
      // assignShards(asignmentMap);
      // } else {
      // LOG.warn("No nodes left for shard redistribution.");
      // }
      // }
      // TODO assign this to new nodes
    }
  }

  protected class NodeListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentNodes) throws KattaException {
      LOG.info("got node event: " + currentNodes);
      try {
        List<String> newNodes = ComparisonUtil.getNew(_nodes, currentNodes);
        if (!newNodes.isEmpty()) {
          LOG.info(newNodes.size() + " new node/s connected: " + newNodes);
          _distributeShardThread.updateNodes(currentNodes);
        }

        final List<String> disconnectedNodes = ComparisonUtil.getRemoved(_nodes, currentNodes);
        if (!disconnectedNodes.isEmpty()) {
          LOG.info(disconnectedNodes.size() + " node/s disconnected: " + disconnectedNodes);
          _distributeShardThread.updateNodes(currentNodes);

          // get all indexes which the node carried
          Set<String> serverdIndexes = new HashSet<String>();
          for (String node : disconnectedNodes) {
            String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
            List<String> shards = _zkClient.getChildren(node2ShardRootPath);
            for (String shard : shards) {
              AssignedShard assignedShard = new AssignedShard();
              _zkClient.readData(ZkPathes.getNode2ShardPath(node, shard), assignedShard);
              serverdIndexes.add(assignedShard.getIndexName());
            }
          }

          // rebalance them
          for (String index : serverdIndexes) {
            _distributeShardThread.addIndex(index);
          }
        }

        _nodes = currentNodes;
      } finally {
        _zkClient.getSyncMutex().notifyAll();
      }
    }
  }

  protected class IndexListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentIndexes) throws KattaException {
      LOG.info("got index event: " + currentIndexes);
      try {
        final List<String> removedIndices = ComparisonUtil.getRemoved(_indexes, currentIndexes);
        for (String index : removedIndices) {
          try {
            _distributeShardThread.removeIndex(index);
          } catch (Exception e) {
            LOG.error("could not undeploy index '" + index + "' properly", e);
            // TODO jz: should we set the state to undeploy error ??
          }
        }
        final List<String> addedIndexes = ComparisonUtil.getNew(_indexes, currentIndexes);
        for (String newIndex : addedIndexes) {
          _distributeShardThread.addIndex(newIndex);
        }
        _indexes = currentIndexes;
      } finally {
        _zkClient.getSyncMutex().notifyAll();
      }
    }
  }

  protected class MasterListener implements IZkDataListener<MasterMetaData> {

    public void handleDataAdded(String dataPath, MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataChange(String dataPath, MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataDeleted(String dataPath) throws KattaException {
      if (!_isMaster) {
        // start from scratch again...
        LOG.info("An master failure was detected...");
        try {
          start();
        } catch (final KattaException e) {
          LOG.error("Faild to process Master change notificaiton.", e);
        }
      }
    }

    public MasterMetaData createWritable() {
      return new MasterMetaData();
    }

  }

  protected List<String> readNodes() throws KattaException {
    return _zkClient.getChildren(ZkPathes.NODES);
  }

  protected List<String> readIndexes() throws KattaException {
    return _zkClient.getChildren(ZkPathes.INDEXES);
  }

  public boolean isMaster() {
    return _isMaster;
  }

  public List<String> getNodes() {
    return Collections.unmodifiableList(_nodes);
  }

  public List<String> getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

}
