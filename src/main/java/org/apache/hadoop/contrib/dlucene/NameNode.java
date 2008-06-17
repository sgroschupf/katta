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
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.data.MakeReplicationPlan;
import org.apache.hadoop.contrib.dlucene.data.NameNodeData;
import org.apache.hadoop.contrib.dlucene.data.NameNodeLeaseManager;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

/**
 * The namenode maintains the set of indexes that are available for search,
 * keeps track of which datanode should handle changes to an index and initiates
 * index synchronization between datanodes. The namenode can be configured to
 * replicate indexes a specified number of times.
 */
public class NameNode implements ClientToNameNodeProtocol,
    DataNodeToNameNodeProtocol {

  private final Lock datanodeInfoLock = new ReentrantLock();

  private NameNodeData nameNodeData = null;

  /** The current replication plan. */
  private Map<InetSocketAddress, IndexLocation[]> replicationPlan = null;

  /** is replication on ? */
  private final static boolean USE_REPLICATION = true;

  private NameNodeLeaseManager leaseManager = null;
  

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
  protected void join() {
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
  public void offerService() throws IOException {
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
   * @param hostname the hostname
   * @param port the port number
   * @param configuration the Hadoop configuration
   * @param nodeType is this a NameNode or a DataNode?
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
   * @param className the name of the class
   */
  protected static void printUsage(String className) {
    System.err.println("Usage: java " + className);
    System.err.println("           [-r, --rack <network location>] |");
  }

  /**
   * Parse command line arguments.
   * 
   * @param args command line arguments
   * @param conf the Hadoop Configuration
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
   * @see org.apache.hadoop.ipc.VersionedProtocol#getProtocolVersion(java.lang.String,
   *      long)
   */
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(ClientToNameNodeProtocol.class.getName())) {
      return ClientToNameNodeProtocol.VERSION_ID;
    } else if (protocol.equals(DataNodeToNameNodeProtocol.class.getName())) {
      return DataNodeToNameNodeProtocol.VERSION_ID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.DataNodeToNameNodeProtocol#getLease(org.apache.hadoop.dlucene.IndexVersion, java.net.InetSocketAddress)
   */
  public Lease getLease(IndexLocation index)
      throws IOException {
    return leaseManager.getLease(index.getIndexVersion(), index.getAddress());
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.dlucene.DataNodeToNameNodeProtocol#relinquishLease(org.apache.hadoop.dlucene.Lease)
   */
  public boolean relinquishLease(Lease lease) throws IOException {
    return leaseManager.relinquishLease(lease);
  }

  /**
   * Constructor.
   * 
   * @param configuration the Hadoop configuration
   * @param addr
   * @throws Exception
   */
  NameNode(Configuration configuration, InetSocketAddress addr)
      throws Exception {
    this.heartBeatInterval = 1000L * configuration.getLong(
        Constants.HEARTBEAT_INTERVAL_NAME, Constants.HEARTBEAT_INTERVAL_VALUE);
    this.nodeAddr = addr;
    nameNodeData = new NameNodeData(configuration);
    init(addr.getHostName(), addr.getPort(), configuration,
        Constants.NAMENODE_DEFAULT_NAME);
    leaseManager = new NameNodeLeaseManager(1000L * configuration.getLong(
        Constants.LEASE_LENGTH_NAME, Constants.LEASE_LENGTH_VALUE));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.AbstractNode#run()
   */
  private class HeartBeater implements Runnable {
    public void run() {
      LOG.info("DataNode.Replicator.run is running on " + nodeAddr.toString());
      try {
        offerService();
      } catch (Exception e) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        shutdown();
      }
      LOG.info("Finishing NameNode");
    }
  }

  /**
   * Startup the node from the command line.
   * 
   * @param args You can use -r to specify the rack that the node is on
   */
  public static void main(String args[]) {
    Configuration conf = new Configuration();
    try {
      NameNode namenode = null;
      StringUtils.startupShutdownMessage(NameNode.class, args, LOG);
      if (!parseArguments(args, conf)) {
        printUsage(NameNode.class.toString());
      } else {
        InetSocketAddress addr = NetUtils.createSocketAddr(conf.get(
            Constants.NAMENODE_DEFAULT_NAME,
            Constants.NAMENODE_DEFAULT_NAME_VALUE));
        namenode = createNode(conf, addr);
        if (namenode != null) {
          namenode.join();
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * Create the node.
   * 
   * @param conf configuration
   * @param addr the socket address of the node
   * @return
   * @throws Exception
   */
  protected static NameNode createNode(Configuration conf,
      InetSocketAddress addr) throws Exception {
    NameNode dn = new NameNode(conf, addr);
    dn.initThread();
    return dn;
  }

  /**
   * Initialize the threads.
   */
  private void initThread() {
    heartBeatClass = new HeartBeater();
    heartBeatThread = new Thread(heartBeatClass, NameNode.class.toString()
        + ": ");
    heartBeatThread.setDaemon(true); // needed for JUnit testing
    heartBeatThread.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.AbstractNode#shutdown()
   */
  public void shutdown() {
    LOG.info("Shutting down NameNode");
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
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.protocols.DataNodeToNameNodeProtocol#heartbeat(org.apache.hadoop.dlucene.DataNodeStatusInformation,
   *      org.apache.hadoop.dlucene.IndexLocation[])
   */
  public HeartbeatResponse heartbeat(DataNodeStatus status,
      IndexLocation[] searchableIndexes, Lease[] leases) {
    datanodeInfoLock.lock();
    HeartbeatResponse hbr = null;
    try {
      LOG.info("Namenode received heartbeat");
      nameNodeData.add(status, searchableIndexes);
      leaseManager.updateLeases(leases);
    } finally {
      datanodeInfoLock.unlock();
    }
    // do we have a replication plan to return?
    if (status.getAddress() != null && replicationPlan != null) {
      hbr = new HeartbeatResponse(replicationPlan.get(status.getAddress()),
          null);
      return hbr;
    }
    return null;
  }

  /**
   * Build the replication plan.
   * 
   * @throws Exception
   */
  void doHeartbeat() throws IOException {
    datanodeInfoLock.lock();
    try {
      leaseManager.checkLeases();
      nameNodeData.doFailureDetect();
      if (USE_REPLICATION) {
        MakeReplicationPlan rp = new MakeReplicationPlan(nameNodeData);
        replicationPlan = rp.createReplicationPlan();
      }
    } finally {
      datanodeInfoLock.unlock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append("datanodeInfoLock: " + datanodeInfoLock.toString() + "\n");
    if (replicationPlan != null) {
      result.append("replicationPlan: " + replicationPlan.entrySet().toString()
          + "\n");
    } else {
      result.append("replicationPlan: null");
    }
    result.append(nameNodeData.toString());
    return result.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.ClientToNameNodeProtocol#getDataNode()
   */
  public String getDataNode() {
    return nameNodeData.getRandomDataNode();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.dlucene.ClientToNameNodeProtocol#getSearchableIndexes()
   */
  public IndexLocation[] getSearchableIndexes() {
    return nameNodeData.getSearchableIndexes();
  }
}
