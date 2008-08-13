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
package net.sf.katta;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;

import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import com.yahoo.zookeeper.server.ServerStats;
import com.yahoo.zookeeper.server.ZkNioFactory;
import com.yahoo.zookeeper.server.ZooKeeperServer;
import com.yahoo.zookeeper.server.NIOServerCnxn.Factory;
import com.yahoo.zookeeper.server.quorum.QuorumPeer;
import com.yahoo.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class ZkServer {

  private QuorumPeer _quorumPeer;

  private ZooKeeperServer _zk;

  private Factory _nioFactory;

  public ZkServer(final ZkConfiguration conf) throws KattaException {
    if (Logger.isInfo()) {
      final String[] localHostNames = NetworkUtil.getLocalHostNames();
      String names = "";
      for (int i = 0; i < localHostNames.length; i++) {
        final String name = localHostNames[i];
        names += " " + name;
        if (i + 1 != localHostNames.length) {
          names += ",";
        }
      }
      Logger.info("Starting ZkServer on: [" + names + "] ...");
    }
    startZooKeeperServer(conf);
  }

  // check if this server has to start a zookeeper server
  private void startZooKeeperServer(final ZkConfiguration conf) throws KattaException {
    final String[] localhostHostNames = NetworkUtil.getLocalHostNames();
    final String servers = conf.getZKServers();
    // check if this server needs to start a _client server.
    int pos = -1;
    Logger.debug("check if hostNames " + servers + " is in list: " + Arrays.asList(localhostHostNames));
    if ((pos = NetworkUtil.hostNamesInList(servers, localhostHostNames)) != -1) {
      // yes this server needs to start a zookeeper server
      final String[] hosts = servers.split(",");
      final String[] hostSplitted = hosts[pos].split(":");
      int port = 2181;
      if (hostSplitted.length > 1) {
        port = Integer.parseInt(hostSplitted[1]);
      }
      // check if this machine is already something running..
      if (isPortFree(port)) {
        final int tickTime = conf.getZKTickTime();
        final File dataDir = conf.getZKDataDir();
        final File dataLogDir = conf.getZKDataLogDir();
        dataDir.mkdirs();
        dataLogDir.mkdirs();

        if (hosts.length > 1) {
          // multiple zk servers
          startQuorumPeer(conf, localhostHostNames, hosts, tickTime, dataDir, dataLogDir);
          Logger.info("Distributed zookeeper server started...");
        } else {
          // single zk server
          startSingleZkServer(tickTime, dataDir, dataLogDir, port);
          Logger.info("Single zookeeper server started...");
        }
        // now if required we initialize our namespace
        final ZKClient client = new ZKClient(conf);
        client.start(300000);
        // TODO jz: do we initialize the client only for creating the namespaces
        // ?? We should at least then close the client, huh ?
      } else {
        Logger.error("Zookeeper port was already in use. Running in single machine mode?");
      }
    }
  }

  private boolean isPortFree(final int port) {
    try {
      final ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  private void startSingleZkServer(final int tickTime, final File dataDir, final File dataLogDir, final int port) {
    try {
      ServerStats.registerAsConcrete();
      _zk = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
      _zk.startup();
      // _nioFactory = new NIOServerCnxn.Factory(port);
      _nioFactory = new ZkNioFactory(port);
      _nioFactory.setZooKeeperServer(_zk);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to start single ZooKeeper server.", e);
    } catch (final InterruptedException e) {
      Logger.warn("ZooKeeper server was interrupted.", e);
    }
  }

  private void startQuorumPeer(final ZkConfiguration conf, final String[] localhostHostNames, final String[] hosts,
      final int tickTime, final File dataDir, final File dataLogDir) {
    Logger.info("Starting ZooKeeper ZkServer...");
    final ArrayList<QuorumServer> peers = new ArrayList<QuorumServer>();
    long myId = -1;
    int myPort = -1;
    for (int i = 0; i < hosts.length; i++) {
      final String[] hostAndPort = hosts[i].split(":");
      final String host = hostAndPort[0];
      final int port = Integer.parseInt(hostAndPort[1]);
      final InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
      peers.add(new QuorumServer(i, inetSocketAddress));
      if (NetworkUtil.hostNameInArray(localhostHostNames, host)) {
        myId = i;
        myPort = port;
      }
    }

    final int initLimit = conf.getZKInitLimit();
    final int syncLimit = conf.getZKSyncLimit();

    final int electionAlg = 0;
    final int clientPort = conf.getZKClientPort();
    try {
      _quorumPeer = new QuorumPeer(peers, dataDir, dataLogDir, clientPort, electionAlg, myPort, myId, tickTime,
          initLimit, syncLimit);
      _quorumPeer.start();
    } catch (final IOException e) {
      throw new RuntimeException("Could not start QuorumPeer ZooKeeper server.", e);
    }
  }

  public void join() {
    if (_quorumPeer != null) {
      try {
        _quorumPeer.join();
      } catch (final InterruptedException e) {
        Logger.info("QuorumPeer was interruped.", e);
      } finally {
        _quorumPeer.shutdown();
      }
    } else if (_nioFactory != null) {
      try {
        _nioFactory.join();
      } catch (final InterruptedException e) {
        Logger.info("Nio server was interruped.", e);
      } finally {
        _nioFactory.shutdown();
      }
      _zk.shutdown();
      ServerStats.unregister();
    }
  }

  public static void main(final String[] args) throws KattaException {
    if (args.length != 1) {
      usage();
      System.exit(1);
    }

    final ZkConfiguration conf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(conf);
    zkServer.join();
  }

  private static void usage() {
    System.out.println("net.sf.katta.ZkServer");

  }

  public void shutdown() {
    Logger.info("Shutting down server...");
    if (_nioFactory != null) {
      _nioFactory.shutdown();
    }
    if (_quorumPeer != null) {
      _quorumPeer.shutdown();
    }

    ServerStats.unregister();
  }
}
