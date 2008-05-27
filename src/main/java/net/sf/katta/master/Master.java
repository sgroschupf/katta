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
import net.sf.katta.zk.IZKEventListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.yahoo.zookeeper.proto.WatcherEvent;

public class Master {

  ZKClient _zk;

  protected List<String> _slaves = new ArrayList<String>();

  protected List<String> _indexes = new ArrayList<String>();

  private IDeployPolicy _policy;

  public Master(final ZKClient client) throws KattaException {
    _zk = client;
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
    // create default folder structure.....
    _zk.createDefaultStructure();
    // Announce me as master
    // boot up loading slaves and indexes..
    loadSlaves();
    loadIndexes();
  }

  private void loadIndexes() throws KattaException {
    Logger.info("Loading Indexes...");
    synchronized (_zk.getSyncMutex()) {
      final ArrayList<String> indexes = _zk.subscribeChildChanges(IPaths.INDEXES, new IndexListener());
      assert indexes != null;
      if (indexes.size() > 0) {
        addIndexes(indexes);
      }
    }
  }

  private void loadSlaves() throws KattaException {
    Logger.info("Loading Slaves...");
    synchronized (_zk.getSyncMutex()) {
      final ArrayList<String> children = _zk.subscribeChildChanges(IPaths.SLAVES, new SlaveListener());
      Logger.info("Found Slaves: " + children);
      assert children != null;
      _slaves = children;
    }
  }

  private void addIndexes(final List<String> indexes) throws KattaException {
    Logger.info("Adding indexes: " + indexes);
    for (final String index : indexes) {
      final IndexMetaData metaData = new IndexMetaData();
      _zk.readData(IPaths.INDEXES + "/" + index, metaData);
      deployIndex(index, metaData);
    }
  }

  private void deployIndex(final String index, final IndexMetaData metaData) throws KattaException {
    Logger.info("Deploying index: " + index);
    final ArrayList<AssignedShard> shards = getShardsForIndex(index, metaData);
    if (shards.size() == 0) {
      throw new IllegalArgumentException("No shards in folder found, this is not a vailid katta virtual index.");
    }
    Logger.info("Found Shards: " + shards);
    // add shards to index..
    final String indexPath = IPaths.INDEXES + "/" + index;
    for (final AssignedShard shard : shards) {
      final String path = indexPath + "/" + shard.getShardName();
      if (!_zk.exists(path)) {
        _zk.create(path, shard);
      }
    }

    // compute how to distribute shards to slaves
    final List<String> readSlaves = readSlaves();
    if (readSlaves != null && readSlaves.size() > 0) {
      final Map<String, List<AssignedShard>> distributionMap = _policy.ditribute(_zk, readSlaves, shards);
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
            metaData.setIsDeployed(true);
            _zk.writeData(indexPath, metaData);
          } catch (final KattaException e) {
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
        final List<String> servingSlaves = _zk.getChildren(IPaths.SHARD_TO_SLAVE + "/" + expectedShard.getShardName());
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
        Logger.info("adding shard: " + shardName);
        if (!_zk.exists(shardServerPath)) {
          _zk.create(shardServerPath);
        }
        // slave to shard
        Logger.info("signing shard " + shardName + " to slave: " + slave);
        final String slavePath = IPaths.SLAVE_TO_SHARD + "/" + slave;
        final String slaveShardPath = slavePath + "/" + shardName;
        if (!_zk.exists(slaveShardPath)) {
          _zk.create(slaveShardPath, shard);
        }
      }
    }
  }

  // private void addSlaves(final List<String> newSlaves) {
  // for (final String slave : newSlaves) {
  // // String slavePath = IPaths.SLAVE_TO_SHARD + "/" + slave;
  // // make sure we have the slave in the slave to shard folder
  // // if (!_zk.exists(slavePath)) {
  // // _zk.create(slavePath);
  // // }
  // }
  // }

  private void removeSlaves(final List<String> removedSlaves) throws KattaException {
    for (final String slave : removedSlaves) {
      // get the shards this slave served...
      final String slaveToRemove = IPaths.SLAVE_TO_SHARD + "/" + slave;
      final List<String> toAsignShards = _zk.getChildren(slaveToRemove);
      final List<AssignedShard> shards = new ArrayList<AssignedShard>();
      for (final String shardName : toAsignShards) {
        final AssignedShard metaData = new AssignedShard();
        _zk.readData(slaveToRemove + "/" + shardName, metaData);
        shards.add(metaData);
      }
      _zk.deleteRecursiv(slaveToRemove);
      if (toAsignShards.size() != 0) {
        final Map<String, List<AssignedShard>> asignmentMap = _policy.ditribute(_zk, readSlaves(), shards);
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
    synchronized (_zk.getSyncMutex()) {
      final List<String> slaves = _zk.getChildren(IPaths.SLAVE_TO_SHARD);
      for (final String slave : slaves) {
        final String slavePath = IPaths.SLAVE_TO_SHARD + "/" + slave;
        final List<String> assignedShards = _zk.getChildren(slavePath);
        for (final String shard : assignedShards) {
          final AssignedShard shardWritable = new AssignedShard();
          final String shardPath = slavePath + "/" + shard;
          _zk.readData(shardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _zk.delete(shardPath);
          }
        }
      }
    }
  }

  private class SlaveListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_zk.getSyncMutex()) {
        List<String> currentSlaves;
        try {
          currentSlaves = _zk.getChildren(event.getPath());
          final List<String> removedSlaves = ComparisonUtil.getRemoved(_slaves, currentSlaves);
          removeSlaves(removedSlaves);
          final List<String> newSlaves = ComparisonUtil.getNew(_slaves, currentSlaves);
          // addSlaves(newSlaves);
          _slaves = currentSlaves;
          _zk.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Faled to read zookeeper data.", e);
        }
      }
    }
  }

  private class IndexListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_zk.getSyncMutex()) {
        List<String> freshIndexes;
        try {
          freshIndexes = _zk.getChildren(event.getPath());
          final List<String> removedIndices = ComparisonUtil.getRemoved(_indexes, freshIndexes);
          removeIndexes(removedIndices);
          final List<String> newIndexes = ComparisonUtil.getNew(_indexes, freshIndexes);
          addIndexes(newIndexes);
          _indexes = freshIndexes;
          _zk.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Faild to read zookeeper data", e);
        }
      }
    }
  }

  protected List<String> readSlaves() throws KattaException {
    return _zk.getChildren(IPaths.SLAVES);
  }

  protected List<String> readIndexes() throws KattaException {
    return _zk.getChildren(IPaths.INDEXES);
  }

  @Override
  protected void finalize() throws Throwable {
    _zk.delete(IPaths.MASTER);
    super.finalize();
  }

}
