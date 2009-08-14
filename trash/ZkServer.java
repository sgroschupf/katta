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
package net.sf.katta.zk;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.ZkConfiguration;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

public class ZkServer {

  private final static Logger LOG = Logger.getLogger(ZkServer.class);

  public static final int DEFAULT_PORT = 2181;

  private QuorumPeer _quorumPeer;
  private ZooKeeperServer _zk;
  private Factory _nioFactory;

  public ZkServer(final ZkConfiguration conf) throws KattaException {
    final String[] localHostNames = NetworkUtil.getLocalHostNames();
    String names = "";
    for (int i = 0; i < localHostNames.length; i++) {
      final String name = localHostNames[i];
      names += " " + name;
      if (i + 1 != localHostNames.length) {
        names += ",";
      }
    }
    LOG.info("Starting ZkServer on: [" + names + "] ...");
    startZooKeeperServer(conf);
  }

  // check if this server has to start a zookeeper server
  private void startZooKeeperServer(final ZkConfiguration conf) throws KattaException {
    final String[] localhostHostNames = NetworkUtil.getLocalHostNames();
    final String servers = conf.getZKServers();
    // check if this server needs to start a _client server.
    int pos = -1;
    LOG.debug("check if hostNames " + servers + " is in list: " + Arrays.asList(localhostHostNames));
    if ((pos = NetworkUtil.hostNamesInList(servers, localhostHostNames)) != -1) {
      // yes this server needs to start a zookeeper server
      final String[] hosts = servers.split(",");
      final String[] hostSplitted = hosts[pos].split(":");
      int port = DEFAULT_PORT;
      if (hostSplitted.length > 1) {
        port = Integer.parseInt(hostSplitted[1]);
      }
      // check if this machine is already something running..
      if (NetworkUtil.isPortFree(port)) {
        final int tickTime = conf.getZKTickTime();
        final File dataDir = conf.getZKDataDir();
        final File dataLogDir = conf.getZKDataLogDir();
        dataDir.mkdirs();
        dataLogDir.mkdirs();

        if (hosts.length > 1) {
          // multiple zk servers
          LOG.info("Start distributed zookeeper server...");
          startQuorumPeer(conf, localhostHostNames, hosts, tickTime, dataDir, dataLogDir);
        } else {
          // single zk server
          LOG.info("Start single zookeeper server...");
          startSingleZkServer(tickTime, dataDir, dataLogDir, port);
        }
        LOG.info("data dir: " + dataDir.getAbsolutePath());
        LOG.info("data log dir: " + dataLogDir.getAbsolutePath());
      } else {
        // TODO jz: shoudn't we better throw an exception
        LOG.error("Zookeeper port " + port + " was already in use. Running in single machine mode?");
      }
    }
  }

  private void startSingleZkServer(final int tickTime, final File dataDir, final File dataLogDir, final int port) {
    try {
//      ServerStats.registerAsConcrete();
      _zk = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
      _nioFactory = new NIOServerCnxn.Factory(port);
      // _nioFactory = new ZkNioFactory(port);
      _nioFactory.startup(_zk);
    } catch (final IOException e) {
      throw new RuntimeException("Unable to start single ZooKeeper server.", e);
    } catch (final InterruptedException e) {
      LOG.warn("ZooKeeper server was interrupted.", e);
    }
  }

  private void startQuorumPeer(final ZkConfiguration conf, final String[] localhostHostNames, final String[] hosts,
      final int tickTime, final File dataDir, final File dataLogDir) {
    LOG.info("Starting ZooKeeper ZkServer...");
//    final ArrayList<QuorumServer> peers = new ArrayList<QuorumServer>();
    HashMap<Long, QuorumServer> peers = new HashMap<Long, QuorumServer>();
    long myId = -1;
    
    for (int i = 0; i < hosts.length; i++) {
      final String[] hostAndPort = hosts[i].split(":");
      final String host = hostAndPort[0];
      final int port = Integer.parseInt(hostAndPort[1]);
      final InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
      peers.put(new Long(i), new QuorumServer(i, inetSocketAddress));
      if (NetworkUtil.hostNameInArray(localhostHostNames, host)) {
        myId = i;
      }
    }

    final int initLimit = conf.getZKInitLimit();
    final int syncLimit = conf.getZKSyncLimit();

    final int electionAlg = 0;
    final int clientPort = conf.getZKClientPort();
    try {
    	_quorumPeer = new QuorumPeer(peers, dataDir, dataLogDir, clientPort, electionAlg, myId, tickTime, initLimit, syncLimit);
//      _quorumPeer = new QuorumPeer(peers, dataDir, dataLogDir, clientPort, electionAlg,myId, myPort, myId, tickTime,
//          initLimit, syncLimit);
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
        LOG.info("QuorumPeer was interruped.", e);
      } finally {
        _quorumPeer.shutdown();
      }
    } else if (_nioFactory != null) {
      try {
        _nioFactory.join();
      } catch (final InterruptedException e) {
        LOG.info("Nio server was interruped.", e);
      } finally {
        _nioFactory.shutdown();
      }
      _zk.shutdown();
//      ServerStats.unregister();
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
    LOG.info("Shutting down server...");
    if (_nioFactory != null) {
      _nioFactory.shutdown();
    }
    if (_quorumPeer != null) {
      _quorumPeer.shutdown();
    }

//    ServerStats.unregister();
  }
}
