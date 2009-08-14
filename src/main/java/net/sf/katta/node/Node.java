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

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.ShardError;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Node implements IZkReconnectListener {

  protected final static Logger LOG = Logger.getLogger(Node.class);

  public static final long _protocolVersion = 0;

  protected ZkConfiguration _conf;
  protected ZkClient _zkClient;
  private Server _rpcServer;
  private INodeManaged _server;

  protected String _nodeName;
  protected int _rpcServerPort;
  protected File _shardsFolder;
  // contains the deploy errors two
  protected final Set<String> _deployedShards = new HashSet<String>();

  private Timer _timer;
  protected final long _startTime = System.currentTimeMillis();
  protected long _queryCounter;

  private final NodeConfiguration _configuration;
  private NodeState _currentState;

  public static enum NodeState {
    STARTING, RECONNECTING, IN_SERVICE, LOST;
  }

  public Node(final ZkClient zkClient, INodeManaged server) {
    this(zkClient, new NodeConfiguration(), server);
  }

  public Node(final ZKClient zkClient, final NodeConfiguration configuration, INodeManaged server) {
    if (server == null) {
      throw new IllegalArgumentException("Null server passed to Node()");
    }
    _conf = zkClient.getConfig();
    _zkClient = zkClient;
    _configuration = configuration;
    _server = server;
    _zkClient.subscribeReconnects(this);
    LOG.info("Starting node, server class = " + server.getClass().getCanonicalName());
  }

  /**
   * Boots the node
   * 
   * @throws KattaException
   */
  public void start() throws KattaException {
    LOG.debug("Starting node...");

    try {
      _zkClient.getEventLock().lock();
      LOG.debug("Starting rpc server...");
      _nodeName = startRPCServer(_configuration.getStartPort());
      _server.setNodeName(_nodeName);

      // we add hostName and port to the shardFolder to allow multiple nodes per
      // server with the same configuration
      _shardsFolder = new File(_configuration.getShardFolder(), _nodeName.replaceAll(":", "@"));

      if (!_shardsFolder.exists()) {
        _shardsFolder.mkdirs();
      }
      if (!_shardsFolder.exists()) {
        throw new IllegalStateException("could not create local shard folder '" + _shardsFolder.getAbsolutePath() + "'");
      }

      LOG.debug("Starting zk client...");
      if (!_zkClient.isStarted()) {
        _zkClient.start(30000);
      }
      cleanupLocalShardFolder();
      announceNode(NodeState.STARTING);
      startShardServing(false);

      LOG.info("Started node: " + _nodeName + "...");
      updateStatus(NodeState.IN_SERVICE);
      _timer = new Timer("QueryCounter", true);
      _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  public void handleNewSession() throws Exception {
    announceNode(NodeState.RECONNECTING);
    cleanupLocalShardFolder();
    startShardServing(true);
    updateStatus(NodeState.IN_SERVICE);
  }

  public void handleStateChanged(KeeperState state) throws Exception {
    // do nothing
  }

  private void cleanupLocalShardFolder() throws KattaException {
    String node2ShardRootPath = _conf.getZKNodeToShardPath(_nodeName);
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

  /*
   * Writes node ephemeral data into zookeeper
   */
  private void announceNode(NodeState nodeState) throws KattaException {
    LOG.info("Announce node '" + _nodeName + "'...");
    final NodeMetaData metaData = new NodeMetaData(_nodeName, nodeState);
    final String nodePath = _conf.getZKNodePath(_nodeName);
    if (_zkClient.exists(nodePath)) {
      LOG.warn("Old node path '" + nodePath + "' for this node detected, deleting it...");
      _zkClient.delete(nodePath);
    }

    final String nodeToShardPath = _conf.getZKNodeToShardPath(_nodeName);
    if (!_zkClient.exists(nodeToShardPath)) {
      _zkClient.create(nodeToShardPath);
    }
    _zkClient.createEphemeral(nodePath, metaData);
    LOG.info("Node '" + _nodeName + "' announced");
  }

  private void startShardServing(boolean restart) throws KattaException {
    LOG.info("Start serving shards...");
    final String nodeToShardPath = _conf.getZKNodeToShardPath(_nodeName);
    List<String> shardsNames = _zkClient.subscribeChildChanges(nodeToShardPath, new ShardListener());

    if (restart) {
      List<String> removed = CollectionUtil.getListOfRemoved(_deployedShards, shardsNames);
      undeployShards(removed);
    }
    ArrayList<AssignedShard> assignedShards = readAssignedShards(shardsNames);
    
    deployShards(assignedShards);
    _deployedShards.clear();
    _deployedShards.addAll(shardsNames);
  }

  protected void deployShards(final List<AssignedShard> newShards) throws KattaException {
    for (AssignedShard shard : newShards) {
      String shardName = shard.getShardName();
      File localShardFolder = getLocalShardFolder(shardName);
      try {
        if (!localShardFolder.exists()) {
          installShard(shard, localShardFolder);
        }
        _server.addShard(shardName, localShardFolder);
        announceShard(shard);
      } catch (Throwable t) {
        LOG.error(_nodeName + ": could not deploy shard '" + shard + "'", t);
        ShardError shardError = new ShardError(t.getMessage());
        String shard2ErrorPath = _conf.getZKShardToErrorPath(shardName, _nodeName);
        if (_zkClient.exists(shard2ErrorPath)) {
          LOG.warn("detected old shard-to-error entry - deleting it..");
          // must be an old ephemeral
          _zkClient.delete(shard2ErrorPath);
        }
        _zkClient.createEphemeral(shard2ErrorPath, shardError);
        FileUtil.deleteFolder(localShardFolder);
      }
    }
  }

  protected void undeployShards(final List<String> shardsToRemove) {
    for (String shard : shardsToRemove) {
      try {
        LOG.info("Undeploying shard: " + shard);
        _server.removeShard(shard);
        String shard2NodePath = _conf.getZKShardToNodePath(shard, _nodeName);
        if (_zkClient.exists(shard2NodePath)) {
          _zkClient.delete(shard2NodePath);
        }
        FileUtil.deleteFolder(getLocalShardFolder(shard));
      } catch (final Exception e) {
        LOG.error("Failed to undeploy shard: " + shard, e);
      }
    }
  }

  /*
   * Announce in zookeeper node is serving this shard,
   */
  private void announceShard(AssignedShard shard) throws KattaException {
    String shardName = shard.getShardName();
    LOG.info("announce shard '" + shardName + "'");
    // announce that this node serves this shard now...
    final String shard2NodePath = _conf.getZKShardToNodePath(shardName, _nodeName);
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2NodePath);
    }

    Map<String, String> metaData;
    try {
      metaData = _server.getShardMetaData(shardName);
    } catch (Throwable t) {
      throw new KattaException("Error measuring shard size for " + shardName, t);
    }
    DeployedShard deployedShard = new DeployedShard(shardName, metaData);
    _zkClient.createEphemeral(shard2NodePath, deployedShard);
  }

  /*
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content.
   */
  private void installShard(AssignedShard shard, File localShardFolder) throws KattaException {
    final String shardPath = shard.getShardPath();
    String shardName = shard.getShardName();
    LOG.info("install shard '" + shardName+ "' from " + shardPath);
    // TODO sg: to fix HADOOP-4422 we try to download the shard 5 times
    int maxTries = 5;
    for (int i = 0; i < maxTries; i++) {
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

        // Looks like we were successful.
        if (i > 0) {
          LOG.error("Loaded shard:" + shard);
        }
        return;
      } catch (final URISyntaxException e) {
        throw new KattaException("Can not parse uri for path: " + shardPath, e);
      } catch (final Exception e) {
        LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardPath, i, maxTries), e);
        if (i >= maxTries - 1) {
          throw new KattaException("Can not load shard: " + shardPath, e);
        }
      }
    }
  }

  public void shutdown() {
    LOG.info("shutdown " + _nodeName + " ...");
    try {
      _zkClient.getEventLock().lock();
      try {
        // we deleting the ephemeral's since this is the fastest and the safest
        // way, but if this does not work, it shouldn't be too bad
        _zkClient.delete(_conf.getZKNodePath(_nodeName));
        for (String shard : _deployedShards) {
          String shard2NodePath = _conf.getZKShardToNodePath(shard, _nodeName);
          String shard2ErrorPath = _conf.getZKShardToErrorPath(shard, _nodeName);
          _zkClient.deleteIfExists(shard2NodePath);
          _zkClient.deleteIfExists(shard2ErrorPath);
        }
      } catch (Throwable t) {
        LOG.warn("could'nt cleanup zk ephemeral Paths: " + t.getMessage());
      }
      _timer.cancel();
      _zkClient.unsubscribeAll();
      _zkClient.close();
      _rpcServer.stop();
      _rpcServer = null;
      try {
        _server.shutdown();
      } catch (Throwable t) {
        LOG.error("Error shutting down server", t);
      }
      _server = null;
    } finally {
      _zkClient.getEventLock().unlock();
    }
    LOG.info("shutdown " + _nodeName + " finished");
  }

  public String getName() {
    return _nodeName;
  }

  public int getRPCServerPort() {
    return _rpcServerPort;
  }

  public NodeState getState() {
    return _currentState;
  }

  public void join() throws InterruptedException {
    _rpcServer.join();
  }

  public Collection<String> getDeployedShards() {
    return _deployedShards;
  }

  public Server getRpcServer() {
    return _rpcServer;
  }

  /*
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of node.server.port.start + 10000
   */
  private String startRPCServer(final int startPort) {
    final String hostName = NetworkUtil.getLocalhostName();
    int serverPort = startPort;
    int tryCount = 10000;
    while (_rpcServer == null) {
      try {
        _rpcServer = RPC.getServer(_server, "0.0.0.0", serverPort, new Configuration());
        LOG.info(_server.getClass().getSimpleName() + " server started on : " + hostName + ":" + serverPort);
        _rpcServerPort = serverPort;
      } catch (final BindException e) {
        if (serverPort - startPort < tryCount) {
          serverPort++;
          // try again
        } else {
          throw new RuntimeException("tried " + tryCount + " ports and no one is free...");
        }
      } catch (final IOException e) {
        throw new RuntimeException("unable to create rpc server", e);
      }
    }
    try {
      _rpcServer.start();
    } catch (final IOException e) {
      throw new RuntimeException("failed to start rpc server", e);
    }
    return hostName + ":" + serverPort;
  }

  private File getLocalShardFolder(final String shardName) {
    return new File(_shardsFolder, shardName);
  }




  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    shutdown();
  }

  @Override
  public String toString() {
    return _nodeName;
  }

  private void updateStatus(NodeState state) throws KattaException {
    _currentState = state;
    final String nodePath = _conf.getZKNodePath(_nodeName);
    final NodeMetaData metaData = new NodeMetaData();
    _zkClient.readData(nodePath, metaData);
    metaData.setState(state);
    _zkClient.writeData(nodePath, metaData);
  }
  
  private ArrayList<AssignedShard> readAssignedShards(final List<String> shardsToDeploy) throws KattaException {
    ArrayList<AssignedShard> newShards = new ArrayList<AssignedShard>();
    for (String shardName : shardsToDeploy) {
      AssignedShard assignedShard = new AssignedShard();
      _zkClient.readData(_conf.getZKNodeToShardPath(_nodeName, shardName), assignedShard);  
      newShards.add(assignedShard);
    }
    return newShards;
  }
  /*
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
      undeployShards(shardsToUndeploy);
      // we actually want to get all shard information now to make sure it can not be changed during any other steps
      
      ArrayList<AssignedShard> newShards = readAssignedShards(shardsToDeploy);
      deployShards(newShards);
    }

   

  }

  /*
   * A Thread that updates the status of the node within zookeeper.
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
      final String nodePath = _conf.getZKNodePath(_nodeName);
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

}
