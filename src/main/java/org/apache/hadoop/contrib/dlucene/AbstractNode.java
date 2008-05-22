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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

/**
 * Base class for NameNode and DataNode.
 */
abstract class AbstractNode {

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
   * Constructor.
   * 
   * @param configuration the Hadoop configuration
   */
  protected AbstractNode(Configuration configuration) {
    this.heartBeatInterval = 1000L * configuration.getLong(
        Constants.HEARTBEAT_INTERVAL_NAME, Constants.HEARTBEAT_INTERVAL_VALUE);
  }

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
   * The task that the node does at a regular intervals e.g. send heartbeats,
   * process heartbeats etc.
   * 
   * @throws IOException
   */
  abstract void doHeartbeat() throws IOException;

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
   * Shutdown the node.
   */
  public void shutdown() {
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
}
