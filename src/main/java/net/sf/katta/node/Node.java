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

import net.sf.katta.node.monitor.IMonitor;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.operation.node.ShardRedeployOperation;
import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ThrottledInputStream.ThrottleSemaphore;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.NetworkUtil;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

public class Node implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Node.class);

  private final NodeConfiguration _nodeConf;
  protected InteractionProtocol _protocol;
  private final IContentServer _contentServer;
  protected NodeContext _context;
  protected String _nodeName;
  private Server _rpcServer;

  private IMonitor _monitor;
  private Thread _nodeOperatorThread;
  private boolean _stopped;

  public Node(InteractionProtocol protocol, IContentServer server) {
    this(protocol, new NodeConfiguration(), server);
  }

  public Node(InteractionProtocol protocol, final NodeConfiguration configuration, IContentServer _nodeManaged) {
    _protocol = protocol;
    this._contentServer = _nodeManaged;
    if (_nodeManaged == null) {
      throw new IllegalArgumentException("Null server passed to Node()");
    }
    _nodeConf = configuration;
  }

  /**
   * Boots the node
   */
  public void start() {
    if (_stopped) {
      throw new IllegalStateException("Node cannot be started again after it was shutdown.");
    }
    LOG.info("starting rpc server with  server class = " + _contentServer.getClass().getCanonicalName());
    String hostName = NetworkUtil.getLocalhostName();
    _rpcServer = startRPCServer(hostName, _nodeConf.getStartPort(), _contentServer);
    _nodeName = hostName + ":" + _rpcServer.getListenerAddress().getPort();
    _contentServer.setNodeName(_nodeName);

    // we add hostName and port to the shardFolder to allow multiple nodes per
    // server with the same configuration
    File shardsFolder = new File(_nodeConf.getShardFolder(), _nodeName.replaceAll(":", "@"));
    int throttleInKbPerSec = _nodeConf.getShardDeployThrottle();
    final ShardManager shardManager;
    if (throttleInKbPerSec > 0) {
      LOG.info("throtteling of shard deployment to " + throttleInKbPerSec + " kilo-bytes per second");
      shardManager = new ShardManager(shardsFolder, new ThrottleSemaphore(throttleInKbPerSec * 1024));
    } else {
      shardManager = new ShardManager(shardsFolder);
    }
    _context = new NodeContext(_protocol, this, shardManager, _contentServer);
    _protocol.registerComponent(this);

    startMonitor(_nodeName, _nodeConf);
    init();
    LOG.info("started node '" + _nodeName + "'");
  }

  private void init() {
    redeployInstalledShards();
    NodeMetaData nodeMetaData = new NodeMetaData(_nodeName);
    NodeQueue nodeOperationQueue = _protocol.publishNode(this, nodeMetaData);
    startOperatorThread(nodeOperationQueue);
  }

  private void startOperatorThread(NodeQueue nodeOperationQueue) {
    _nodeOperatorThread = new Thread(new NodeOperationProcessor(nodeOperationQueue, _context));
    _nodeOperatorThread.setName(NodeOperationProcessor.class.getSimpleName() + ": " + getName());
    _nodeOperatorThread.setDaemon(true);
    _nodeOperatorThread.start();
  }

  @Override
  public synchronized void reconnect() {
    LOG.info(_nodeName + " reconnected");
    init();
  }

  @Override
  public synchronized void disconnect() {
    LOG.info(_nodeName + " disconnected");
    _nodeOperatorThread.interrupt();
    try {
      _nodeOperatorThread.join();
    } catch (InterruptedException e) {
      Thread.interrupted();
    }
    // we keep serving the shards
  }

  private void redeployInstalledShards() {
    Collection<String> installedShards = _context.getShardManager().getInstalledShards();
    ShardRedeployOperation redeployOperation = new ShardRedeployOperation(installedShards);
    try {
      redeployOperation.execute(_context);
    } catch (InterruptedException e) {
      ExceptionUtil.convertToRuntimeException(e);
    }
  }

  private void startMonitor(String nodeName, NodeConfiguration conf) {
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
    if (_stopped) {
      throw new IllegalStateException("already stopped");
    }
    LOG.info("shutdown " + _nodeName + " ...");
    _stopped = true;

    if (_monitor != null) {
      _monitor.stopMonitoring();
    }
    _nodeOperatorThread.interrupt();
    try {
      _nodeOperatorThread.join();
    } catch (InterruptedException e) {
      Thread.interrupted();// proceed
    }

    _protocol.unregisterComponent(this);
    _rpcServer.stop();
    try {
      _context.getContentServer().shutdown();
    } catch (Throwable t) {
      LOG.error("Error shutting down server", t);
    }
    LOG.info("shutdown " + _nodeName + " finished");
  }

  public String getName() {
    return _nodeName;
  }

  public NodeContext getContext() {
    return _context;
  }

  public int getRPCServerPort() {
    return _rpcServer.getListenerAddress().getPort();
  }

  public boolean isRunning() {
    // TODO jz: improve this whole start/stop/isRunning thing
    return _context != null && !_stopped;
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
  private static Server startRPCServer(String hostName, final int startPort, IContentServer nodeManaged) {
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

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  private static class NodeOperationProcessor implements Runnable {

    private final NodeQueue _queue;
    private final NodeContext _nodeContext;

    public NodeOperationProcessor(NodeQueue queue, NodeContext nodeContext) {
      _queue = queue;
      _nodeContext = nodeContext;
    }

    @Override
    public void run() {
      try {
        while (_nodeContext.getNode().isRunning()) {
          NodeOperation operation = _queue.peek();
          OperationResult operationResult;
          try {
            LOG.info("executing " + operation);
            operationResult = operation.execute(_nodeContext);
          } catch (Exception e) {
            LOG.error(_nodeContext.getNode().getName() + ": failed to execute " + operation, e);
            operationResult = new OperationResult(_nodeContext.getNode().getName(), e);
          }
          _queue.complete(operationResult);// only remove after finish
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
      } catch (ZkInterruptedException e) {
        Thread.interrupted();
      }
      LOG.info("node operation processor for " + _nodeContext.getNode().getName() + " stopped");
    }
  }

}
