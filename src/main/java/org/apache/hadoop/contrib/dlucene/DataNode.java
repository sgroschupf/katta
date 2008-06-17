/**
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
package org.apache.hadoop.contrib.dlucene;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.master.IPaths;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.IZKEventListener;
import net.sf.katta.zk.ZKClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.data.DataNodeIndexHandler;
import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.contrib.dlucene.writable.WDocument;
import org.apache.hadoop.contrib.dlucene.writable.WQuery;
import org.apache.hadoop.contrib.dlucene.writable.WSort;
import org.apache.hadoop.contrib.dlucene.writable.WTerm;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.yahoo.zookeeper.proto.WatcherEvent;

/**
 * Implements a datanode that stores Lucene indexes.
 */
public class DataNode extends Node implements DataNodeToDataNodeProtocol,
    ClientToDataNodeProtocol {

  /** Data structure storing index information. */
  private DataNodeIndexHandler data = null;

  /** Status information on this datanode. */
  private DataNodeStatus filesystemStatus = null;

  /** Controls shared access to data structure. */
  private final Lock lock = new ReentrantLock();

  /** Thread for handling replication requests. */
  private Thread replicationThread = null;

  /** The replication object. */
  private Runnable replicator = null;

  /** Log file for this node. */
  protected static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.AbstractNode");

  /** The thread for the heartbeat of this node. */
  protected Thread heartBeatThread = null;

  /** Should the threads keep running? */
  protected volatile boolean shouldRun = true;

  /** The heart beat interval, determined from the configuration file. */
  protected long heartBeatInterval;

  /** The RPC server used by the node. */
  private Server server;

  /** The number of RPC handlers used by this node. */
  private int handlerCount = 2;

  /** Responsible for heartbeat. */
  protected Runnable heartBeatClass = null;

  /** The address of the node. */
  protected InetSocketAddress nodeAddr = null;

  /**
   * Join the heartbeat thread.
   */
  public void join() {
    if (heartBeatThread != null) {
      try {
        heartBeatThread.join();
      } catch (InterruptedException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  /**
   * Main loop for the node. Runs until shutdown.
   * 
   * @throws IOException
   */
  public void offerService() throws IOException, KattaException {
    while (shouldRun) {
      LOG.info("In DataNode.Heartbeater.offerService is running on "
          + nodeAddr.toString());
      try {
        doHeartbeat();
        try {
          long sleep = heartBeatInterval;
          LOG.info(nodeAddr.toString() + " is alive");
          Thread.sleep(sleep);
        } catch (InterruptedException ie) {
          // don't worry if this is interrupted
        }
      } catch (RemoteException re) {
        LOG.warn(StringUtils.stringifyException(re));
        shutdown();
        return;
      }
    }
  }

  /**
   * Initialize the server.
   * 
   * @param hostname
   *          the hostname
   * @param port
   *          the port number
   * @param configuration
   *          the Hadoop configuration
   * @param nodeType
   *          is this a NameNode or a DataNode?
   * @throws IOException
   */
  protected void init(String hostname, int port, Configuration configuration,
      String nodeType) throws IOException {
    this.handlerCount = configuration.getInt(Constants.HANDLER_COUNT_NAME,
        Constants.HANDLER_COUNT_VALUE);
    this.server = RPC.getServer(this, hostname, port, handlerCount, false,
        configuration);

    try {
      this.server.start(); // start RPC server
    } catch (IOException e) {
      this.server.stop();
      throw e;
    }

  }

  /**
   * Print the command line arguments.
   * 
   * @param className
   *          the name of the class
   */
  protected static void printUsage(String className) {
    System.err.println("Usage: java " + className);
    System.err.println("           [-r, --rack <network location>] |");
  }

  /**
   * Parse command line arguments.
   * 
   * @param args
   *          command line arguments
   * @param conf
   *          the Hadoop Configuration
   * @return could the arguments be parsed?
   */
  protected static boolean parseArguments(String[] args, Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    String networkLoc = null;
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        if (i == args.length - 1)
          return false;
        networkLoc = args[++i];
        if (networkLoc.startsWith("-"))
          return false;
      } else {
        return false;
      }
    }
    if (networkLoc != null)
      conf.set("dlucene.datanode.rack", NodeBase.normalize(networkLoc));
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.AbstractNode#doHeartbeat()
   */
  protected void doHeartbeat() throws IOException, KattaException {
    filesystemStatus.updateUsage();
    lock.lock();
    try {
      updateStatus("OK");
      final String path = IPaths.NODES + Constants.zkSeparator + _node;
      _client.writeData(path + Constants.zkSeparator
          + Constants.zkFilesystemStatus, filesystemStatus);
      for (IndexLocation il : data.getIndexes()) {
        _client.writeData(path + Constants.zkSeparator + Constants.zkIndexes
            + Constants.zkSeparator + il.getIndexVersion().getName()
            + Constants.zkSeparator + il.getIndexVersion().getVersion(), il);
      }
      IndexLocation toReplicate = new IndexLocation();
      _client.readData(path + Constants.zkSeparator
          + Constants.zkReplicationPlan, toReplicate);
      filesystemStatus.addReplicationTask(toReplicate);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Perform replication tasks.
   * 
   * @throws IOException
   */
  protected void doReplication() throws IOException {
    while (shouldRun) {
      LOG.info("DataNode.Replicator.doReplication is running on "
          + nodeAddr.toString());
      try {
        if (filesystemStatus.getReplicationTasks().size() > 0) {
          IndexLocation indexToReplicate = filesystemStatus
              .getNextReplicationTask();
          // need to check that index has not already been replicated
          data.copyRemoteIndex(indexToReplicate);
          filesystemStatus.removeReplicationTask(indexToReplicate);
          LOG.info(indexToReplicate + " has finished replicating\n");
        }
        try {
          long sleep = heartBeatInterval;
          LOG.info(nodeAddr.toString() + " is alive");
          Thread.sleep(sleep);
        } catch (InterruptedException ie) {
          // 
        }
      } catch (RemoteException re) {
        LOG.warn(StringUtils.stringifyException(re));
        shutdown();
        return;
      }
    }
  }

  /**
   * Constructor.
   * 
   * @param configuration
   *          the Hadoop configuration
   * @param dataNodeAddress
   *          the address of this node
   * @param nameNodeAddress
   *          the address of the NameNode
   * @param useRamIndex
   *          whether to use a RAM based index or not
   * @throws IOException
   */
  protected DataNode(final ZKClient client, Configuration configuration,
      InetSocketAddress dataNodeAddress, InetSocketAddress nameNodeAddress,
      boolean useRamIndex) throws IOException {
    super(client);
    this.heartBeatInterval = 1000L * configuration.getLong(
        Constants.HEARTBEAT_INTERVAL_NAME, Constants.HEARTBEAT_INTERVAL_VALUE);
    this.nodeAddr = dataNodeAddress;
    // find the name of this machine

    // create the data structure listing indexes on this machine
    String rack = configuration.get(Constants.DATANODE_RACK_NAME);
    if (rack == null) // exec network script or set the default rack
      rack = Network.getNetworkLoc(configuration);
    String root = null;
    if (System.getProperty(Constants.DEFAULT_ROOT_DIR) != null) {
      root = System.getProperty(Constants.DEFAULT_ROOT_DIR);
    } else {
      root = configuration.get(Constants.DEFAULT_ROOT_DIR_NAME,
          Constants.DEFAULT_ROOT_DIR);
    }
    DataNodeConfiguration dataconf = new DataNodeConfiguration(dataNodeAddress,
        rack, root);

    // find the network location of this machine
    filesystemStatus = new DataNodeStatus(dataconf, configuration);

    data = new DataNodeIndexHandler(client, dataconf, configuration,
        new StandardAnalyzer(), useRamIndex);
    init(dataNodeAddress.getHostName(), dataNodeAddress.getPort(),
        configuration, Constants.DATANODE_DEFAULT_NAME);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.DataNodeToDataNodeProtocol#getFileContent(org.apache.hadoop.dlucene.IndexVersion,
   *      java.lang.String)
   */
  public byte[] getFileContent(IndexVersion indexVersion, String file)
      throws IOException {
    Utils.checkArgs(indexVersion, file);
    return data.getFileContent(indexVersion, file);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.DataNodeToDataNodeProtocol#getFileSet(org.apache.hadoop.dlucene.IndexVersion)
   */
  public String[] getFileSet(IndexVersion indexVersion) throws IOException {
    Utils.checkArgs(indexVersion);
    return data.getFileSet(indexVersion);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.ipc.VersionedProtocol#getProtocolVersion(java.lang.String,
   *      long)
   */
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    Utils.checkArgs(protocol);
    if (protocol.equals(DataNodeToDataNodeProtocol.class.getName())) {
      return DataNodeToDataNodeProtocol.VERSION_ID;
    } else if (protocol.equals(ClientToDataNodeProtocol.class.getName())) {
      return ClientToDataNodeProtocol.VERSION_ID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.ClientToDataNodeProtocol#addDocument(java.lang.String,
   *      org.apache.hadoop.dlucene.writable.WDocument)
   */
  public void addDocument(String index, WDocument doc) throws IOException,
      KattaException {
    Utils.checkArgs(index, doc);
    LOG.debug("Adding document to index " + index);
    data.addDocument(index, doc.getDocument());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.ClientToDataNodeProtocol#addIndex(java.lang.String,
   *      org.apache.hadoop.dlucene.IndexLocation)
   */
  public void addIndex(String index, IndexLocation indexToAdd)
      throws IOException, KattaException {
    Utils.checkArgs(index, indexToAdd);
    lock.lock();
    try {
      data.addIndex(index, indexToAdd);
    } finally {
      lock.unlock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.ClientToDataNodeProtocol#commitVersion(java.lang.String)
   */
  public IndexVersion commitVersion(String id) throws IOException,
      KattaException {
    Utils.checkArgs(id);
    IndexVersion result = null;
    lock.lock();
    try {
      result = data.commitVersion(id);
      numberOfCommits++;
      LOG.debug("Committing " + id + " number of commits " + numberOfCommits
          + " to version " + result.toString() + " on "
          + nodeAddr.getHostName() + ":" + nodeAddr.getPort());
    } finally {
      lock.unlock();
    }
    doHeartbeat();
    return result;
  }

  static int numberOfCommits = 0;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.ClientToDataNodeProtocol#removeDocuments(java.lang.String,
   *      org.apache.lucene.index.Term)
   */
  public int removeDocuments(String index, WTerm term) throws KattaException,
      IOException {
    Utils.checkArgs(index, term);
    return data.removeDocuments(index, term.getTerm());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.ClientToDataNodeProtocol#search(org.apache.hadoop.dlucene.data.IndexVersion,
   *      org.apache.lucene.search.Query, org.apache.lucene.search.Sort, int)
   */
  public SearchResults search(IndexVersion i, WQuery query, WSort sort, int n)
      throws IOException {
    Utils.checkArgs(i, query, sort);
    return data.search(i, query.getQuery(), sort.getSort(), n);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.ClientToDataNodeProtocol#addIndex(java.lang.String)
   */
  public IndexVersion createIndex(String index) throws IOException, KattaException {
    Utils.checkArgs(index);
    IndexVersion result = null;
    LOG.debug("Datanode creating index " + index);
    lock.lock();
    try {
      result = data.createIndex(index);
    } finally {
      lock.unlock();
    }
    LOG.debug("Datanode created index with result " + result);
    doHeartbeat();
    return result;
  }

  public int size(String index) throws IOException {
    Utils.checkArgs(index);
    return data.size(index);
  }

  /**
   * Startup the node from the command line.
   * 
   * @param args
   *          You can use -r to specify the rack that the node is on
   */
  public static void main(String[] args) {
    Configuration conf = new Configuration();
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = null;
      if (!parseArguments(args, conf)) {
        printUsage(DataNode.class.toString());
      } else {
        InetSocketAddress dataNodeAddr = NetUtils.createSocketAddr(conf.get(
            Constants.DATANODE_DEFAULT_NAME,
            Constants.DATANODE_DEFAULT_NAME_VALUE));
        InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr(conf.get(
            Constants.NAMENODE_DEFAULT_NAME,
            Constants.NAMENODE_DEFAULT_NAME_VALUE));
        final ZKClient zkclient = new ZKClient(new ZkConfiguration());
        datanode = createNode(zkclient, conf, dataNodeAddr, nameNodeAddr, false);
        if (datanode != null)
          datanode.join();
      }
    } catch (Throwable e) {
      e.printStackTrace();
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * Create a node.
   * 
   * @param configuration
   *          the Hadoop configuration
   * @param dataNodeAddress
   *          the address of this node
   * @param nameNodeAddress
   *          the address of the NameNode
   * @param useRamIndex
   *          whether to use a RAM based index or not
   * @return a DataNode instance
   * @throws IOException
   */
  protected static DataNode createNode(final ZKClient client,
      Configuration configuration, InetSocketAddress dataNodeAddress,
      InetSocketAddress nameNodeAddress, boolean useRamIndex)
      throws IOException {
    DataNode dn = new DataNode(client, configuration, dataNodeAddress,
        nameNodeAddress, useRamIndex);

    // set up thread for sending heartbeats
    dn.initThreads();
    return dn;
  }

  /**
   * Initialize the threads.
   */
  private void initThreads() {
    heartBeatClass = new HeartBeater();
    heartBeatThread = new Thread(heartBeatClass, DataNode.class.toString()
        + ": heartbeat thread");
    heartBeatThread.setDaemon(true); // needed for JUnit testing
    heartBeatThread.start();

    replicator = new Replicator();
    replicationThread = new Thread(replicator, DataNode.class.toString()
        + ": replication thread");
    replicationThread.setDaemon(true);
    replicationThread.start();
  }

  /**
   * The class that performs replication.
   */
  private class Replicator implements Runnable {
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
      LOG.info("DataNode.Replicator.run is running on " + nodeAddr.toString());
      try {
        doReplication();
      } catch (Exception e) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        shutdown();
      }
      LOG.info("Finishing DataNode in: " + data);
    }
  }

  /**
   * The class that performs heartbeats.
   */
  private class HeartBeater implements Runnable {
    public void run() {
      LOG.info("In DataNode.Heartbeater.run is running on "
          + nodeAddr.toString());
      try {
        offerService();
      } catch (Exception e) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        shutdown();
      }
      LOG.info("Finishing DataNode in: " + data);
    }
  }

  /**
   * Stop the processes used by the node.
   */
  public void shutdown() {
    LOG.info("Shutting down DataNode");
    this.shouldRun = false;

    if (heartBeatThread != null) {
      if (heartBeatThread.isAlive()) {
        heartBeatThread.interrupt();
        try {
          heartBeatThread.join();
        } catch (InterruptedException ie) {
          LOG.error(StringUtils.stringifyException(ie));
        }
      }
    }
    this.server.stop();
    if (replicationThread != null) {
      if (replicationThread.isAlive()) {
        replicationThread.interrupt();
      }
    }
  }

  /*
   * Listens to events within the nodeToShard zookeeper folder. Those events are
   * fired if a shard is assigned or removed for this node.
   */
  private class DLuceneShardListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        if (Logger.isDebug()) {
          Logger.debug("ShardListener.process()" + _node);
        }
        final String path = event.getPath();
        List<String> newList;
        try {
          newList = _client.getChildren(path);
          final List<String> shardsToRemove = ComparisonUtil.getRemoved(
              _deployedShards, newList);
          removeShards(shardsToRemove);
          final List<String> shardsToServe = ComparisonUtil.getNew(
              _deployedShards, newList);
          deployAndAnnounceShards(shardsToServe);
          _client.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Failed to read zookeeper information");
        }
      }
    }
  }

  /*
   * A Thread that updates the status of the node within zookeeper.
   */
  private class DLuceneStatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_node != null) {
        long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
        time = Math.max(time, 1);
        final float qpm = (float) _queryCounter / time;
        final NodeMetaData metaData = new NodeMetaData();
        final String path = IPaths.NODES + "/" + _node;
        try {
          if (_client.exists(path)) {
            _client.readData(path, metaData);
            metaData.setQueriesPerMinute(qpm);
            _client.writeData(path, metaData);
          }
        } catch (final KattaException e) {
          Logger.error("Failed to update data in zookeeper", e);
        }
      }
    }
  }
}
