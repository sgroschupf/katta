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
package net.sf.katta.node;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.ShardError;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public abstract class BaseNode extends BaseRpcServer implements IZkReconnectListener {

  protected final static Logger LOG = Logger.getLogger(BaseNode.class);

  public static final long _protocolVersion = 0;

  private ZKClient _zkClient;
  private String _nodeName;

  protected File _shardsFolder;
  private final Set<String> _deployedShards = new HashSet<String>();

  private Timer _timer;
  private final long _startTime = System.currentTimeMillis();
  private long _queryCounter;

  private final NodeConfiguration _configuration;
  private NodeState _currentState;

  public static enum NodeState {
    STARTING, RECONNECTING, IN_SERVICE, LOST;
  }

  public BaseNode(final ZKClient zkClient, final NodeConfiguration configuration) {
    _zkClient = zkClient;
    _configuration = configuration;
    _zkClient.subscribeReconnects(this);
  }

  // to implement by subclasses

  protected abstract void tearDown();

  protected abstract void undeploy(String shard) throws IOException;

  protected abstract void deploy(String shardName, File localShardFolder) throws IOException;

  protected abstract Map<String, String> getMetaData(String shardName);

  // getters

  public String getName() {
    return _nodeName;
  }

  public int getSearchServerPort() {
    return getRpcServerPort();
  }

  public NodeState getState() {
    return _currentState;
  }

  public Collection<String> getDeployedShards() {
    return _deployedShards;
  }

  private File getLocalShardFolder(final String shardName) {
    return new File(_shardsFolder, shardName);
  }

  public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
    return _protocolVersion;
  }

  /**
   * Boots the node
   */
  public void start() throws KattaException {
    LOG.debug("Starting node...");

    try {
      _zkClient.getEventLock().lock();
      LOG.debug("Starting rpc search server...");
      _nodeName = startRpcServer(_configuration.getStartPort());

      // we add hostName and port to the shardFolder to allow multiple nodes per
      // server with the same configuration
      _shardsFolder = new File(_configuration.getShardFolder(), _nodeName.replaceAll(":", "@"));

      if (!_shardsFolder.exists()) {
        if (!_shardsFolder.mkdirs()) {
          // could not create folder
          String msg = "Could not create local shard folder '" + _shardsFolder.getAbsolutePath() + "'";
          throw new IllegalStateException(msg);
        }
      }

      LOG.debug("Starting zk client...");
      if (!_zkClient.isStarted()) {
        _zkClient.start(30000);
      }

      cleanupLocalWorkDir();
      announceNode(NodeState.STARTING);
      startServing(false);

      LOG.info("Started node: " + _nodeName + "...");

      updateStatus(NodeState.IN_SERVICE);
      _timer = new Timer("QueryCounter", true);
      _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);

    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  /**
   * Deletes those shard directories that are not assigned to the node. In most
   * cases that are all su directory of the working folder.
   */
  private void cleanupLocalWorkDir() throws KattaException {
    String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    List<String> shardsToServe = Collections.emptyList();

    if (_zkClient.exists(node2ShardRootPath)) {
      shardsToServe = _zkClient.getChildren(node2ShardRootPath);
    }

    String[] folderList = _shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
    if (folderList != null) {
      List<String> localShards = Arrays.asList(folderList);

      List<String> shardsToRemove = CollectionUtil.getListOfRemoved(localShards, shardsToServe);
      for (String shard : shardsToRemove) {
        File localShard = getLocalShardFolder(shard);
        LOG.info("delete local shard " + localShard.getAbsolutePath());
        FileUtil.deleteFolder(localShard);
      }
    }
  }

  /**
   * Writes node ephemeral data into zookeeper
   */
  private void announceNode(NodeState nodeState) throws KattaException {
    LOG.info("announce node '" + _nodeName + "'...");
    final NodeMetaData metaData = new NodeMetaData(_nodeName, nodeState);
    final String nodePath = ZkPathes.getNodePath(_nodeName);
    if (_zkClient.exists(nodePath)) {
      LOG.warn("Old node path '" + nodePath + "' for this node detected, delete it...");
      _zkClient.delete(nodePath);
    }

    final String nodeToShardPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    if (!_zkClient.exists(nodeToShardPath)) {
      _zkClient.create(nodeToShardPath);
    }
    _zkClient.createEphemeral(nodePath, metaData);
    LOG.info("node '" + _nodeName + "' announced");
  }

  /**
   * Reads the shards data from zookeeper and deploy shards.
   */
  private void startServing(boolean restart) throws KattaException {
    LOG.info("Start serving shards...");
    final String nodeToShardPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    List<String> shardsNames = _zkClient.subscribeChildChanges(nodeToShardPath, new ShardListener());

    if (restart) {
      List<String> removed = CollectionUtil.getListOfRemoved(_deployedShards, shardsNames);
      undeploy(removed);
    }
    ArrayList<AssignedShard> assignedShards = readAssignedShards(shardsNames);

    deploy(assignedShards);
    _deployedShards.clear();
    _deployedShards.addAll(shardsNames);
  }

  /**
   * Invokes undeploy in subclass, remove shard folder from working folder and
   * remove shard to node association in zookeeper
   */
  protected void undeploy(List<String> removed) {
    for (String shard : removed) {
      try {
        LOG.info("Undeploying shard: " + shard);

        undeploy(shard);

        String shard2NodePath = ZkPathes.getShard2NodePath(shard, _nodeName);
        if (_zkClient.exists(shard2NodePath)) {
          _zkClient.delete(shard2NodePath);
        }
        FileUtil.deleteFolder(getLocalShardFolder(shard));
      } catch (final Exception e) {
        LOG.error("Failed to undeploy shard: " + shard, e);
      }
    }
  }

  /**
   * Downloads a shard from remote file system, invokes deploy in subclass and
   * write shard announcement into zookeeper
   */
  protected void deploy(final List<AssignedShard> newShards) throws KattaException {
    for (AssignedShard shard : newShards) {
      String shardName = shard.getShardName();
      File localShardFolder = getLocalShardFolder(shardName);
      try {
        if (!localShardFolder.exists()) {
          download(shard, localShardFolder);
        }
        deploy(shardName, localShardFolder);
        announce(shard);
      } catch (Exception e) {
        LOG.error(_nodeName + ": could not deploy shard '" + shard + "'", e);
        ShardError shardError = new ShardError(e.getMessage());
        String shard2ErrorPath = ZkPathes.getShard2ErrorPath(shardName, _nodeName);
        if (_zkClient.exists(shard2ErrorPath)) {
          LOG.warn("Detected old shard-to-error entry - deleting it..");
          // must be an old ephemeral
          _zkClient.delete(shard2ErrorPath);
        }
        _zkClient.createEphemeral(shard2ErrorPath, shardError);
        FileUtil.deleteFolder(localShardFolder);
      }
    }
  }

  /**
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content.
   */
  private void download(AssignedShard shard, File localShardFolder) throws KattaException {
    final String shardPath = shard.getShardPath();
    String shardName = shard.getShardName();
    LOG.info("Downloading shard '" + shardName + "' from " + shardPath);
    URI uri;
    try {
      uri = new URI(shardPath);
      final FileSystem fileSystem = FileSystem.get(uri, new Configuration());
      final Path path = new Path(shardPath);
      boolean isZip = fileSystem.isFile(path) && shardPath.endsWith(".zip");

      File shardTmpFolder = new File(localShardFolder.getAbsolutePath() + "_tmp");
      // we download extract first to tmp dir in case something went wrong
      FileUtil.deleteFolder(localShardFolder);
      FileUtil.deleteFolder(shardTmpFolder);

      if (isZip) {
        final File shardZipLocal = new File(_shardsFolder, shardName + ".zip");
        if (shardZipLocal.exists()) {
          // make sure we overwrite cleanly
          shardZipLocal.delete();
        }
        fileSystem.copyToLocalFile(path, new Path(shardZipLocal.getAbsolutePath()));
        FileUtil.unzip(shardZipLocal, shardTmpFolder);
        shardZipLocal.delete();
      } else {
        fileSystem.copyToLocalFile(path, new Path(shardTmpFolder.getAbsolutePath()));
      }
      shardTmpFolder.renameTo(localShardFolder);
    } catch (final URISyntaxException e) {
      throw new KattaException("Can not parse uri for path: " + shardPath, e);
    } catch (final IOException e) {
      throw new KattaException("Can not load shard: " + shardPath, e);
    }
  }

  /**
   * Listens to events within the nodeToShard zookeeper folder. Those events are
   * fired if a shard is assigned or removed for this node.
   */
  protected class ShardListener implements IZkChildListener {
    public void handleChildChange(String parentPath, List<String> shardsToServe) throws KattaException {
      LOG.info("got shard event: " + shardsToServe);
      final List<String> shardsToUndeploy = CollectionUtil.getListOfRemoved(_deployedShards, shardsToServe);
      final List<String> shardsToDeploy = CollectionUtil.getListOfAdded(_deployedShards, shardsToServe);
      _deployedShards.removeAll(shardsToUndeploy);
      _deployedShards.addAll(shardsToDeploy);
      undeploy(shardsToUndeploy);
      // we actually want to get all shard information to make sure it can
      // not be changed during any other steps

      ArrayList<AssignedShard> newShards = readAssignedShards(shardsToDeploy);
      deploy(newShards);
    }
  }

  /**
   * Reads shards meta data from zookeeper
   */
  private ArrayList<AssignedShard> readAssignedShards(final List<String> shardsToDeploy) throws KattaException {
    ArrayList<AssignedShard> newShards = new ArrayList<AssignedShard>();
    for (String shardName : shardsToDeploy) {
      AssignedShard assignedShard = new AssignedShard();
      _zkClient.readData(ZkPathes.getNode2ShardPath(_nodeName, shardName), assignedShard);
      newShards.add(assignedShard);
    }
    return newShards;
  }

  /**
   * Announces that shard is servered by this node
   */
  protected void announce(AssignedShard shard) throws KattaException {
    String shardName = shard.getShardName();
    LOG.info("announce shard '" + shardName + "'");
    // announce that this node serves this shard now...
    final String shard2NodePath = ZkPathes.getShard2NodePath(shardName, getName());
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2NodePath);
    }

    DeployedShard deployedShard = new DeployedShard(shardName, getMetaData(shardName));
    _zkClient.createEphemeral(shard2NodePath, deployedShard);
  }

  @Override
  public void handleNewSession() throws Exception {
    announceNode(NodeState.RECONNECTING);
    cleanupLocalWorkDir();
    startServing(true);
    updateStatus(NodeState.IN_SERVICE);
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    // do nothing
  }
  
  /**
   * Cleanly shutdown the node.
   */
  public void shutdown() {
    LOG.info("shutdown " + _nodeName + " ...");
    try {
      _zkClient.getEventLock().lock();
      try {
        // we deleting the ephemeral's since this is the fastest and the safest
        // way, but if this does not work, it shouldn't be too bad
        _zkClient.delete(ZkPathes.getNodePath(_nodeName));
        for (String shard : _deployedShards) {
          String shard2NodePath = ZkPathes.getShard2NodePath(shard, _nodeName);
          String shard2ErrorPath = ZkPathes.getShard2ErrorPath(shard, _nodeName);
          _zkClient.deleteIfExists(shard2NodePath);
          _zkClient.deleteIfExists(shard2ErrorPath);
        }
      } catch (Exception e) {
        LOG.warn("could'nt cleanup zk ephemeral pathes: " + e.getMessage());
      }
      _timer.cancel();
      _zkClient.unsubscribeAll();
      _zkClient.close();
      stopRpcServer();
    } finally {
      _zkClient.getEventLock().unlock();
    }
    LOG.info("shutdown " + _nodeName + " finished");
  }

  /**
   *Updates the status data in zookeeper
   */
  private void updateStatus(NodeState state) throws KattaException {
    _currentState = state;
    final String nodePath = ZkPathes.getNodePath(_nodeName);
    final NodeMetaData metaData = new NodeMetaData();
    _zkClient.readData(nodePath, metaData);
    metaData.setState(state);
    _zkClient.writeData(nodePath, metaData);
  }

  /**
   * Writes periodically node status into zookeeper
   */
  protected class StatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_nodeName != null) {
        // not yet started
        return;
      }
      long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
      time = Math.max(time, 1);
      final float qpm = (float) _queryCounter / time;
      final NodeMetaData metaData = new NodeMetaData();
      final String nodePath = ZkPathes.getNodePath(_nodeName);
      try {
        if (_zkClient.exists(nodePath)) {
          _zkClient.readData(nodePath, metaData);
          metaData.setQueriesPerMinute(qpm);
          _zkClient.writeData(nodePath, metaData);
        }
      } catch (final Exception e) {
        LOG.error("Failed to update node status.", e);
      }
    }
  }

  // util methods

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    shutdown();
  }

  @Override
  public String toString() {
    return _nodeName;
  }

}
