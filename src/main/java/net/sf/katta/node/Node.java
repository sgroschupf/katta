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
import java.util.Collection;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.katta.monitor.IMonitor;
import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.OperationResult;
import net.sf.katta.protocol.operation.node.RedeployShardsOperation;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

public class Node implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Node.class);

  public static final long _protocolVersion = 0;

  protected final InteractionProtocol _protocol;
  private Server _rpcServer;
  private INodeManaged _server;

  protected String _nodeName;

  private Timer _timer;
  protected final long _startTime = System.currentTimeMillis();
  protected long _queryCounter;

  private final NodeConfiguration _nodeConf;
  private NodeState _currentState;

  private IMonitor _monitor;
  private Thread _nodeOperationThread;

  NodeContext _context;

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
    _protocol.registerComponent(this);
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
    String hostName = NetworkUtil.getLocalhostName();
    _rpcServer = startRPCServer(hostName, _nodeConf.getStartPort(), _server);
    _nodeName = hostName + ":" + _rpcServer.getListenerAddress().getPort();
    _server.setNodeName(_nodeName);
    startMonitor(_protocol, _nodeName, _nodeConf);

    // we add hostName and port to the shardFolder to allow multiple nodes per
    // server with the same configuration
    File shardsFolder = new File(_nodeConf.getShardFolder(), _nodeName.replaceAll(":", "@"));
    ShardManager shardManager = new ShardManager(shardsFolder);
    _context = new NodeContext(_protocol, this, shardManager, _server);

    NodeMetaData nodeMetaData = _protocol.getNodeMD(_nodeName);
    if (nodeMetaData == null) {
      nodeMetaData = new NodeMetaData(_nodeName, NodeState.STARTING);
    } else {
      nodeMetaData.setState(NodeState.STARTING);
    }

    // TODO should be done when master answers
    // removeLocalShardsWithoutServeInstruction(nodeMetaData);

    OperationQueue<NodeOperation> nodeOperationQueue = _protocol.publishNode(this, nodeMetaData);
    _nodeOperationThread = new Thread(new NodeOperationProcessor(nodeOperationQueue, _context));
    _nodeOperationThread.setDaemon(true);
    _nodeOperationThread.start();

    // deploy previous served shards
    redeployInstalledShards();

    LOG.info("Started node: " + _nodeName + "...");
    // TODO WelcomeNodeOperation to masters queue
    updateStatus(NodeState.IN_SERVICE);
    _timer = new Timer("QueryCounter", true);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  @Override
  public void reconnect() {
    LOG.info(_nodeName + " reconnected");
    redeployInstalledShards();
    NodeMetaData nodeMetaData = _protocol.getNodeMD(_nodeName);
    nodeMetaData.setState(NodeState.RECONNECTING);
    _protocol.publishNode(this, nodeMetaData);
    updateStatus(NodeState.IN_SERVICE);
  }

  @Override
  public void disconnect() {
    LOG.info(_nodeName + " disconnected");
    // we keep serving the shards
  }

  private void redeployInstalledShards() {
    Collection<String> installedShards = _context.getShardManager().getInstalledShards();
    RedeployShardsOperation redeployOperation = new RedeployShardsOperation(installedShards);
    // _protocol.addNodeOperation(_nodeName, redeployShardsOperation);
    redeployOperation.execute(_context);
  }

  private void startMonitor(InteractionProtocol protocol, String nodeName, NodeConfiguration conf) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("starting node monitor");
    }
    String monitorClass = conf.getMonitorClass();
    try {
      Class<?> c = Class.forName(monitorClass);
      _monitor = (IMonitor) c.newInstance();
      _monitor.startMonitoring(nodeName, _protocol);
    } catch (Exception e) {
      LOG.error("Unable to start node monitor:", e);
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
    _nodeOperationThread.interrupt();
    try {
      _nodeOperationThread.join();
    } catch (InterruptedException e) {
      Thread.interrupted();// proceed
    }

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
    return _rpcServer.getListenerAddress().getPort();
  }

  public NodeState getState() {
    return _currentState;
  }

  public void join() throws InterruptedException {
    _rpcServer.join();
  }

  public Server getRpcServer() {
    return _rpcServer;
  }

  /*
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of node.server.port.start + 10000
   */
  private static Server startRPCServer(String hostName, final int startPort, INodeManaged nodeManaged) {
    int serverPort = startPort;
    int tryCount = 10000;
    Server _rpcServer = null;
    while (_rpcServer == null) {
      try {
        _rpcServer = RPC.getServer(nodeManaged, "0.0.0.0", serverPort, new Configuration());
        LOG.info(nodeManaged.getClass().getSimpleName() + " server started on : " + hostName + ":" + serverPort);
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
    return _rpcServer;
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

  private class NodeOperationProcessor implements Runnable {

    private final OperationQueue<NodeOperation> _distributedBlockingQueue;
    private final NodeContext _nodeContext;

    public NodeOperationProcessor(OperationQueue<NodeOperation> distributedBlockingQueue,
            NodeContext nodeContext) {
      _distributedBlockingQueue = distributedBlockingQueue;
      _nodeContext = nodeContext;
    }

    @Override
    public void run() {
      try {
        while (true) {
          String elementName = _distributedBlockingQueue.peekElementName();
          OperationId operationId = new OperationId(_nodeName, elementName);
          NodeOperation operation = _distributedBlockingQueue.peek();
          OperationResult operationResult;
          try {
            operationResult = operation.execute(_context);
          } catch (Exception e) {
            LOG.error("failed to execute " + operation, e);
            operationResult = new OperationResult(_nodeName, e);
          }
          _protocol.addNodeOperationResult(_context.getNode(), operationId, operationResult);
          _distributedBlockingQueue.poll();// only remove after finish
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      LOG.info("node operation processor stopped");
    }
  }

}
