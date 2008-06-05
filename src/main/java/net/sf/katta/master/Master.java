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

  protected List<String> _slaves = new ArrayList<String>();

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
      // boot up loading slaves and indexes..
      loadSlaves();
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

  private void loadSlaves() throws KattaException {
    Logger.debug("Loading slaves...");
    synchronized (_client.getSyncMutex()) {
      final ArrayList<String> children = _client.subscribeChildChanges(IPaths.SLAVES, new SlaveListener());
      Logger.debug("Found slaves: " + children);
      assert children != null;
      _slaves = children;
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
      throw new IllegalArgumentException("No shards in folder found, this is not a vailid katta virtual index.");
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

    // compute how to distribute shards to slaves
    final List<String> readSlaves = readSlaves();
    if (readSlaves != null && readSlaves.size() > 0) {
      final Map<String, List<AssignedShard>> distributionMap = _policy.ditribute(_client, readSlaves, shards, metaData
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
                Logger.info("Index not yet fully deployed, waiting");
                Thread.sleep(2000);
              } catch (final InterruptedException e) {
                Logger.error("Deployment process was interrupted", e);
                return;
              }
            }
            Logger.info("Finnaly the index is deployed...");
            metaData.setState(IndexMetaData.IndexState.DEPLOYED);
            _client.writeData(indexPath, metaData);
          } catch (final KattaException e) {
            metaData.setState(IndexMetaData.IndexState.DEPLOYED);
            try {
              _client.writeData(indexPath, metaData);
            } catch (final KattaException ke) {
              throw new RuntimeException("Failed to write Index deployError", ke);
            }
            throw new RuntimeException("Failed to write data into zookeeper", e);
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
    final Set<String> slaves = distributionMap.keySet();
    boolean allDeployed = true;
    for (final String slave : slaves) {
      // which shards this slave should serve..
      final List<AssignedShard> shardsToServe = distributionMap.get(slave);
      all += shardsToServe.size();
      for (final AssignedShard expectedShard : shardsToServe) {
        // lookup who is actually serving this shard.
        final List<String> servingSlaves = _client.getChildren(IPaths.SHARD_TO_SLAVE + "/"
            + expectedShard.getShardName());
        // is the slave we expect here already?
        boolean asExpected = false;
        for (final String servingSlave : servingSlaves) {
          if (slave.equals(servingSlave)) {
            asExpected = true;
            deployed++;
            break;
          }
        }
        if (!asExpected) {
          // slave is not yet serving shard
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
    final Set<String> slaves = distributionMap.keySet();
    for (final String slave : slaves) {

      final List<AssignedShard> shardsToServer = distributionMap.get(slave);
      for (final AssignedShard shard : shardsToServer) {
        // shard to server
        final String shardName = shard.getShardName();
        final String shardServerPath = IPaths.SHARD_TO_SLAVE + "/" + shardName;
        if (!_client.exists(shardServerPath)) {
          _client.create(shardServerPath);
        }
        // slave to shard
        Logger.info("Assigning:" + shardName + " to: " + slave);
        final String slavePath = IPaths.SLAVE_TO_SHARD + "/" + slave;
        final String slaveShardPath = slavePath + "/" + shardName;
        if (!_client.exists(slaveShardPath)) {
          _client.create(slaveShardPath, shard);
        }
      }
    }
  }



  private void removeSlaves(final List<String> removedSlaves) throws KattaException {
    for (final String slave : removedSlaves) {
      // get the shards this slave served...
      final String slaveToRemove = IPaths.SLAVE_TO_SHARD + "/" + slave;
      final List<String> toAsignShards = _client.getChildren(slaveToRemove);
      final List<AssignedShard> shards = new ArrayList<AssignedShard>();
      for (final String shardName : toAsignShards) {
        final AssignedShard metaData = new AssignedShard();
        _client.readData(slaveToRemove + "/" + shardName, metaData);
        shards.add(metaData);
      }
      _client.deleteRecursiv(slaveToRemove);
      if (toAsignShards.size() != 0) {
        // since we lost one shard, we want to use replication level 1, since
        // all other replica still exists..
        final Map<String, List<AssignedShard>> asignmentMap = _policy.ditribute(_client, readSlaves(), shards, 1);
        asignShards(asignmentMap);
      }
      // assign this to new slaves
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
   * iterates through all slaves and removes the assigned shards for given
   * indexName
   * 
   * @throws KattaException
   */
  private void removeIndex(final String indexName) throws KattaException {
    synchronized (_client.getSyncMutex()) {
      final List<String> slaves = _client.getChildren(IPaths.SLAVE_TO_SHARD);
      for (final String slave : slaves) {
        final String slavePath = IPaths.SLAVE_TO_SHARD + "/" + slave;
        final List<String> assignedShards = _client.getChildren(slavePath);
        for (final String shard : assignedShards) {
          final AssignedShard shardWritable = new AssignedShard();
          final String shardPath = slavePath + "/" + shard;
          _client.readData(shardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _client.delete(shardPath);
          }
        }
      }
    }
  }

  private class SlaveListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        List<String> currentSlaves;
        try {
          currentSlaves = _client.getChildren(event.getPath());
          final List<String> removedSlaves = ComparisonUtil.getRemoved(_slaves, currentSlaves);
          removeSlaves(removedSlaves);
          final List<String> newSlaves = ComparisonUtil.getNew(_slaves, currentSlaves);
          // addSlaves(newSlaves);
          _slaves = currentSlaves;
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

  protected List<String> readSlaves() throws KattaException {
    return _client.getChildren(IPaths.SLAVES);
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
