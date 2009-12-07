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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.ShardError;
import net.sf.katta.monitor.IMonitor;
import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

public class Node implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Node.class);

  public static final long _protocolVersion = 0;

  private final InteractionProtocol _protocol;
  private Server _rpcServer;
  private INodeManaged _server;

  protected String _nodeName;
  protected int _rpcServerPort;
  protected File _shardsFolder;
  protected final Set<String> _deployedShards = new HashSet<String>();

  private Timer _timer;
  protected final long _startTime = System.currentTimeMillis();
  protected long _queryCounter;

  private final NodeConfiguration _nodeConf;
  private NodeState _currentState;

  private IMonitor _monitor;

  public static enum NodeState {
    STARTING, RECONNECTING, IN_SERVICE, LOST;
  }

  public Node(InteractionProtocol protocol, INodeManaged server) {
    this(protocol, new NodeConfiguration(), server);
  }

  public Node(InteractionProtocol protocol, final NodeConfiguration configuration, INodeManaged server) {
    _protocol = protocol;
    if (server == null) {
      throw new IllegalArgumentException("Null server passed to Node()");
    }
    _nodeConf = configuration;
    _server = server;
    protocol.registerComponent(this);
    LOG.info("Starting node, server class = " + server.getClass().getCanonicalName());
  }

  /**
   * Boots the node
   */
  public void start() {
    if (_server == null) {
      throw new IllegalStateException("Node cannot be started again after it was shutdown.");
    }

    LOG.debug("Starting node...");

    LOG.debug("Starting rpc server...");
    _nodeName = startRPCServer(_nodeConf.getStartPort());
    _server.setNodeName(_nodeName);
    startMonitor(_protocol, _nodeName, _nodeConf);

    // we add hostName and port to the shardFolder to allow multiple nodes per
    // server with the same configuration
    _shardsFolder = new File(_nodeConf.getShardFolder(), _nodeName.replaceAll(":", "@"));

    if (!_shardsFolder.exists()) {
      _shardsFolder.mkdirs();
    }
    if (!_shardsFolder.exists()) {
      throw new IllegalStateException("could not create local shard folder '" + _shardsFolder.getAbsolutePath() + "'");
    }

    cleanupLocalShardFolder();
    _protocol.publishNode(this, NodeState.STARTING);
    startShardServing(false);

    LOG.info("Started node: " + _nodeName + "...");
    updateStatus(NodeState.IN_SERVICE);
    _timer = new Timer("QueryCounter", true);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  private void startMonitor(InteractionProtocol protocol, String nodeName, NodeConfiguration conf) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("starting node monitor");
    }
    String monitorClass = conf.getMonitorClass();
    try {
      Class<?> c = Class.forName(monitorClass);
      _monitor = (IMonitor) c.newInstance();
      _monitor.startMonitoring(nodeName, protocol);
    } catch (Exception e) {
      LOG.error("Unable to start node monitor:", e);
    }
  }

  private void cleanupLocalShardFolder() {
    List<String> shardsToServe = _protocol.getNodeShards(_nodeName);
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

  private void startShardServing(boolean restart) {
    LOG.info("Start serving shards...");
    List<String> shardsNames = _protocol.registerShardListener(this, _nodeName, new IAddRemoveListener() {
      @Override
      public void removed(String shardName) {
        LOG.info("remove shard event: " + shardName);
        undeployShard(shardName);
      }

      @Override
      public void added(String shardName) {
        LOG.info("add shard event: " + shardName);
        deployShard(shardName);
      }

    });

    if (restart) {
      List<String> removed = CollectionUtil.getListOfRemoved(_deployedShards, shardsNames);
      for (String removedShard : removed) {
        undeployShard(removedShard);
      }
    }

    for (String shard : shardsNames) {
      deployShard(shard);
    }
    _deployedShards.clear();
    _deployedShards.addAll(shardsNames);
  }

  protected void deployShard(String shardName) {
    AssignedShard shard = _protocol.getNodeShardMD(_nodeName, shardName);
    File localShardFolder = getLocalShardFolder(shardName);
    try {
      if (!localShardFolder.exists()) {
        installShard(shard, localShardFolder);
      }
      _server.addShard(shardName, localShardFolder);
      announceShard(shard);
      _deployedShards.add(shardName);
    } catch (Throwable t) {
      LOG.error(_nodeName + ": could not deploy shard '" + shard + "'", t);
      ShardError shardError = new ShardError(t.getMessage());
      _protocol.publishShardError(this, shard, shardError);
      FileUtil.deleteFolder(localShardFolder);
    }
  }

  protected void undeployShard(final String shard) {
    try {
      LOG.info("Undeploying shard: " + shard);
      _server.removeShard(shard);
      _protocol.unpublishShard(this, shard);
      FileUtil.deleteFolder(getLocalShardFolder(shard));
      _deployedShards.remove(shard);
    } catch (final Exception e) {
      LOG.error("Failed to undeploy shard: " + shard, e);
    }
  }

  /*
   * Announce in zookeeper node is serving this shard,
   */
  private void announceShard(AssignedShard shard) throws KattaException {
    String shardName = shard.getShardName();
    LOG.info("announce shard '" + shardName + "'");
    Map<String, String> metaData;
    try {
      metaData = _server.getShardMetaData(shardName);
    } catch (Throwable t) {
      throw new KattaException("Error retrieving shard metadata for " + shardName, t);
    }
    _protocol.publishShard(this, shard, metaData);
  }

  /*
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content. If the
   * system property katta.spool.zip.shards is true, the zip file is staged to
   * the local disk before being unzipped.
   */
  private void installShard(AssignedShard shard, File localShardFolder) throws KattaException {
    final String shardPath = shard.getShardPath();
    String shardName = shard.getShardName();
    LOG.info("install shard '" + shardName + "' from " + shardPath);
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
        try {
          FileUtil.deleteFolder(localShardFolder);
          FileUtil.deleteFolder(shardTmpFolder);

          if (isZip) {
            FileUtil.unzip(path, shardTmpFolder, fileSystem, System.getProperty("katta.spool.zip.shards", "false")
                    .equalsIgnoreCase("true"));
          } else {
            fileSystem.copyToLocalFile(path, new Path(shardTmpFolder.getAbsolutePath()));
          }
          shardTmpFolder.renameTo(localShardFolder);
        } finally {
          // Ensure that the tmp folder is deleted on an error
          FileUtil.deleteFolder(shardTmpFolder);
        }
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
    if (_server == null) {
      return;
    }

    LOG.info("shutdown " + _nodeName + " ...");
    if (_monitor != null) {
      _monitor.stopMonitoring();
    }
    _timer.cancel();

    _protocol.unregisterComponent(this);
    _rpcServer.stop();
    _rpcServer = null;
    try {
      _server.shutdown();
    } catch (Throwable t) {
      LOG.error("Error shutting down server", t);
    }
    _server = null;
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

  private void updateStatus(NodeState state) {
    _currentState = state;
    _protocol.updateNodeStatus(_nodeName, state);
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  @Override
  public void reconnect() {
    LOG.info(_nodeName + " reconnected");
    _protocol.publishNode(this, NodeState.RECONNECTING);
    cleanupLocalShardFolder();
    startShardServing(true);
    updateStatus(NodeState.IN_SERVICE);
  }

  @Override
  public void disconnect() {
    LOG.info(_nodeName + " disconnected");
    // we keep serving the shards
  }

  /**
   * A Thread that updates the status of the node within zookeeper.
   * 
   * TODO jz: maybe we should implement this as {@link IMonitor} as well ?
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
      try {
        _protocol.updateNodeStatus(_nodeName, qpm);
      } catch (final Exception e) {
        LOG.error("Failed to update node status.", e);
      }
    }
  }

}
