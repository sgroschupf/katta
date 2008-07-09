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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.zk.IZKEventListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.yahoo.zookeeper.proto.WatcherEvent;

public class Master {

  ZKClient _client;

  protected List<String> _nodes = new ArrayList<String>();

  protected List<String> _indexes = new ArrayList<String>();

  private IDeployPolicy _policy;

  private boolean _isMaster;

  public Master(final ZKClient client) throws KattaException {
    _client = client;
    _client.waitForZooKeeper(300000);
    _client.createDefaultNameSpace();
    final MasterConfiguration masterConfiguration = new MasterConfiguration();
    final String deployPolicy = masterConfiguration.getDeployPolicy();
    try {
      final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicy);
      _policy = policyClazz.newInstance();
    } catch (final Exception e) {
      throw new KattaException("Unable to instantiate deploy policy", e);
    }
  }

  public void start() throws KattaException {

    if (becomeMaster()) {
      // master
      // Announce me as master
      // boot up loading nodes and indexes..
      loadNodes();
      loadIndexes();
      _isMaster = true;
    } else {
      _isMaster = false;
      // secondary master
      _client.subscribeDataChanges(IPaths.MASTER, new MasterListener());
      Logger.info("Secondary Master started...");

    }
  }

  private boolean becomeMaster() {
    synchronized (_client.getSyncMutex()) {
      try {
        final String hostName = NetworkUtil.getLocalhostName();
        final MasterMetaData freshMaster = new MasterMetaData(hostName, System.currentTimeMillis());
        // no master so this one will be master
        if (!_client.exists(IPaths.MASTER)) {
          _client.createEphemeral(IPaths.MASTER, freshMaster);
          Logger.info("Master " + hostName + " started....");
          return true;
        } else {
          return false;
        }
      } catch (final KattaException e) {
        throw new RuntimeException("Failed to communicate with ZooKeeper", e);
      }
    }
  }

  private void loadIndexes() throws KattaException {
    Logger.debug("Loading indexes...");
    synchronized (_client.getSyncMutex()) {
      final ArrayList<String> indexes = _client.subscribeChildChanges(IPaths.INDEXES, new IndexListener());
      assert indexes != null;
      if (indexes.size() > 0) {
        addIndexes(indexes);
      }
    }
  }

  private void loadNodes() throws KattaException {
    Logger.debug("Loading nodes...");
    synchronized (_client.getSyncMutex()) {
      final ArrayList<String> children = _client.subscribeChildChanges(IPaths.NODES, new NodeListener());
      Logger.debug("Found nodes: " + children);
      assert children != null;
      _nodes = children;
    }
  }

  private void addIndexes(final List<String> indexes) throws KattaException {
    Logger.info("Adding indexes: " + indexes);
    for (final String index : indexes) {
      final IndexMetaData metaData = new IndexMetaData();
      _client.readData(IPaths.INDEXES + "/" + index, metaData);
      deployIndex(index, metaData);
    }
  }

  private void deployIndex(final String index, final IndexMetaData metaData) throws KattaException {
    final ArrayList<AssignedShard> shards = getShardsForIndex(index, metaData);
    if (shards.size() == 0) {
      throw new IllegalArgumentException("No shards in folder found, this is not a valid katta virtual index.");
    }
    Logger.info("Deploying index: " + index + " [" + shards + "]");
    // add shards to index..
    final String indexPath = IPaths.INDEXES + "/" + index;
    for (final AssignedShard shard : shards) {
      final String path = indexPath + "/" + shard.getShardName();
      if (!_client.exists(path)) {
        _client.create(path, shard);
      }
    }

    // compute how to distribute shards to nodes
    final List<String> readNodes = readNodes();
    if (readNodes != null && readNodes.size() > 0) {
      final Map<String, List<AssignedShard>> distributionMap = _policy.distribute(_client, readNodes, shards, metaData
          .getReplicationLevel());
      asignShards(distributionMap);
      // lets have a thread watching deployment is things are done we set the
      // flag in the meta data...
      new Thread() {
        @Override
        public void run() {
          try {
            while (!isDeployedAsExpected(distributionMap)) {
              try {
                Logger.info("Index '" + index + "' not yet fully deployed, waiting");
                Thread.sleep(2000);
                if (!_client.exists(IPaths.INDEXES + "/" + index)) {
                  Logger.warn("Index '" + index + "' removed before the deployment completed.");
                  break;
                }
              } catch (final InterruptedException e) {
                Logger.error("Deployment process was interrupted", e);
                return;
              }
            }
            Logger.info("Finnaly the index '" + index + "' is deployed...");
            metaData.setState(IndexMetaData.IndexState.DEPLOYED);
            _client.writeData(indexPath, metaData);
          } catch (final KattaException e) {
            Logger.error("deploy of index '" + index + "' failed.", e);
            metaData.setState(IndexMetaData.IndexState.DEPLOY_ERROR);
            try {
              _client.writeData(indexPath, metaData);
            } catch (final KattaException ke) {
              throw new RuntimeException("Failed to write Index deployError", ke);
            }
          }
        }
      }.start();
    }
  }

  public ArrayList<AssignedShard> getShardsForIndex(final String index, final IndexMetaData metaData) {
    final String pathString = metaData.getPath();
    // get shard folders from source
    URI uri;
    try {
      uri = new URI(pathString);
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException(
          "unable to parse index path uri, make sure it starts with file:// or hdfs:// ", e);
    }
    FileSystem fileSystem;
    try {
      fileSystem = FileSystem.get(uri, new Configuration());
    } catch (final IOException e) {
      throw new IllegalArgumentException(
          "unable to retrive file system, make sure your path starts with hadoop support prefix like file:// or hdfs://");
    }
    final Path path = new Path(pathString);
    final ArrayList<AssignedShard> shards = new ArrayList<AssignedShard>();
    try {
      final FileStatus[] listStatus = fileSystem.listStatus(path, new PathFilter() {
        public boolean accept(final Path path) {
          return !path.getName().startsWith(".");
        }
      });
      for (final FileStatus fileStatus : listStatus) {
        if (fileStatus.isDir() || fileStatus.getPath().toString().endsWith(".zip")) {
          shards.add(new AssignedShard(index, fileStatus.getPath().toString()));
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException("unable to list index path: " + pathString, e);
    }
    return shards;
  }

  private boolean isDeployedAsExpected(final Map<String, List<AssignedShard>> distributionMap) throws KattaException {
    int all = 0;
    int deployed = 0;
    int notDeployed = 0;
    final Set<String> nodes = distributionMap.keySet();
    boolean allDeployed = true;
    for (final String node : nodes) {
      // which shards this node should serve..
      final List<AssignedShard> shardsToServe = distributionMap.get(node);
      all += shardsToServe.size();
      for (final AssignedShard expectedShard : shardsToServe) {
        // lookup who is actually serving this shard.
        final List<String> servingNodes = _client
            .getChildren(IPaths.SHARD_TO_NODE + "/" + expectedShard.getShardName());
        // is the node we expect here already?
        boolean asExpected = false;
        for (final String servingNode : servingNodes) {
          if (node.equals(servingNode)) {
            asExpected = true;
            deployed++;
            break;
          }
        }
        if (!asExpected) {
          // node is not yet serving shard
          notDeployed++;
          allDeployed = false;
        }
      }

    }
    Logger.info("deploying: " + deployed + " of " + all + " deployed, pending: " + notDeployed);
    // all as expected..
    return allDeployed;
  }

  private void asignShards(final Map<String, List<AssignedShard>> distributionMap) throws KattaException {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {

      final List<AssignedShard> shardsToServer = distributionMap.get(node);
      for (final AssignedShard shard : shardsToServer) {
        // shard to server
        final String shardName = shard.getShardName();
        final String shardServerPath = IPaths.SHARD_TO_NODE + "/" + shardName;
        if (!_client.exists(shardServerPath)) {
          _client.create(shardServerPath);
        }
        // node to shard
        Logger.info("Assigning:" + shardName + " to: " + node);
        final String nodePath = IPaths.NODE_TO_SHARD + "/" + node;
        final String nodeShardPath = nodePath + "/" + shardName;
        if (!_client.exists(nodeShardPath)) {
          _client.create(nodeShardPath, shard);
        }
      }
    }
  }

  private void removeNodes(final List<String> removedNodes) throws KattaException {
    for (final String node : removedNodes) {
      // get the shards this node served...
      final String nodeToRemove = IPaths.NODE_TO_SHARD + "/" + node;
      final List<String> toAsignShards = _client.getChildren(nodeToRemove);
      final List<AssignedShard> shards = new ArrayList<AssignedShard>();
      for (final String shardName : toAsignShards) {
        final AssignedShard metaData = new AssignedShard();
        _client.readData(nodeToRemove + "/" + shardName, metaData);
        shards.add(metaData);
      }
      _client.deleteRecursive(nodeToRemove);
      if (toAsignShards.size() != 0) {
        // since we lost one shard, we want to use replication level 1, since
        // all other replica still exists..
        List<String> nodes = readNodes();
        if (nodes.size() > 0) {
          final Map<String, List<AssignedShard>> asignmentMap = _policy.distribute(_client, nodes, shards, 1);
          asignShards(asignmentMap);
        } else {
          Logger.warn("No nodes left for shard redistribution.");
        }
      }
      // assign this to new nodes
    }
  }

  private void removeIndexes(final List<String> removedIndexes) {
    for (final String indexName : removedIndexes) {
      try {
        removeIndex(indexName);
      } catch (final KattaException e) {
        Logger.error("Failed to remove index: " + indexName, e);
      }
    }
  }

  /*
   * iterates through all nodes and removes the assigned shards for given
   * indexName
   * 
   * @throws KattaException
   */
  private void removeIndex(final String indexName) throws KattaException {
    synchronized (_client.getSyncMutex()) {
      final List<String> nodes = _client.getChildren(IPaths.NODE_TO_SHARD);
      for (final String node : nodes) {
        final String nodePath = IPaths.NODE_TO_SHARD + "/" + node;
        final List<String> assignedShards = _client.getChildren(nodePath);
        for (final String shard : assignedShards) {
          final AssignedShard shardWritable = new AssignedShard();
          final String shardPath = nodePath + "/" + shard;
          _client.readData(shardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _client.delete(shardPath);
          }
        }
      }
    }
  }

  private class NodeListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        List<String> currentNodes;
        try {
          currentNodes = _client.getChildren(event.getPath());
          final List<String> removedNodes = ComparisonUtil.getRemoved(_nodes, currentNodes);
          removeNodes(removedNodes);
          ComparisonUtil.getNew(_nodes, currentNodes);
          // addNodes(newNodes);
          _nodes = currentNodes;
          _client.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Faled to read zookeeper data.", e);
        }
      }
    }
  }

  private class IndexListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        List<String> freshIndexes;
        try {
          freshIndexes = _client.getChildren(event.getPath());
          final List<String> removedIndices = ComparisonUtil.getRemoved(_indexes, freshIndexes);
          removeIndexes(removedIndices);
          final List<String> newIndexes = ComparisonUtil.getNew(_indexes, freshIndexes);
          addIndexes(newIndexes);
          _indexes = freshIndexes;
          _client.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Faild to read zookeeper data", e);
        }
      }
    }
  }

  private class MasterListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        // start from scratch again...
        Logger.info("An master failure was detected...");
        try {
          start();
        } catch (final KattaException e) {
          Logger.error("Faild to process Master change notificaiton.", e);
        }
      }
    }
  }

  protected List<String> readNodes() throws KattaException {
    return _client.getChildren(IPaths.NODES);
  }

  protected List<String> readIndexes() throws KattaException {
    return _client.getChildren(IPaths.INDEXES);
  }

  @Override
  protected void finalize() throws Throwable {
    _client.delete(IPaths.MASTER);
    super.finalize();
  }

  public boolean isMaster() {
    return _isMaster;
  }

}
