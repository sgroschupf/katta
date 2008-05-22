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
package org.apache.hadoop.contrib.dlucene.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

/**
 * Class that extends NetworkTopology
 */
public class Network extends NetworkTopology {

  /**
   * Constructor
   */
  public Network() {
    super();
  }

  static Node toNode(DataNodeStatus status) {
    return new NodeBase(toLocation(status));
  }

  static String toLocation(DataNodeStatus status) {
    String name = Network.convertInetSocketAddress(status.getAddress());
    return status.getRack() + NodeBase.PATH_SEPARATOR_STR + name;
  }

  public void add(DataNodeStatus status) {
    super.add(toNode(status));
  }

  public void remove(DataNodeStatus status) {
    super.remove(get(status));
  }

  public boolean isOnSameRack(DataNodeStatus status1, DataNodeStatus status2) {
    return super.isOnSameRack(get(status1), get(status2));
  }

  public Node get(DataNodeStatus status) {
    return super.getNode(toLocation(status));
  }

  public boolean contains(DataNodeStatus status) {
    return super.contains(get(status));
  }

  /**
   * Get a node at random
   * 
   * @param scope the node should be within this scope
   * @param excludedScope the node should not be within this scope
   * @return
   */
  private Collection<InetSocketAddress> getRandomNode(String scope,
      String excludedScope) {
    List<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
    if (excludedScope != null) {
      if (scope.startsWith(excludedScope)) {
        return null;
      }
      if (!excludedScope.startsWith(scope)) {
        excludedScope = null;
      }
    }
    Node scopeNode = getNode(scope);
    if (!(scopeNode instanceof InnerNode)) {
      result.add(NetUtils.createSocketAddr(scopeNode.getName()));
      return result;
    }
    InnerNode innerNode = (InnerNode) scopeNode;
    int numOfDatanodes = innerNode.getNumOfLeaves();
    Node excludedScopeNode = excludedScope != null ? getNode(excludedScope) : null;
    if (numOfDatanodes > 0) {
      Set<String> seen = new HashSet<String>();
      int leafIndex = r.nextInt(numOfDatanodes);
      for (int i = 0; i <= numOfDatanodes ; i++) {
        Node resultNode = innerNode.getLeaf((leafIndex + i) % numOfDatanodes,
            excludedScopeNode);
        if (resultNode != null) {
          if (!seen.contains(resultNode.getName())) {
            LOG.debug("Using index " + i + " and getting back " + resultNode.getName());
            result.add(NetUtils.createSocketAddr(resultNode.getName()));
            seen.add(resultNode.getName());
          }
        }
      }
    }
    return result;
  }

  /**
   * Get a node at random
   * 
   * @param scope The scope the node must be within
   * @return
   */
  public Collection<InetSocketAddress> getRandomNode(String scope) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return getRandomNode(NodeBase.ROOT, scope.substring(1));
      }
      return getRandomNode(scope, null);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Convert an InetSocketAddress to a name
   * 
   * @param addr
   * @return
   */
  public static String convertInetSocketAddress(InetSocketAddress addr) {
    Utils.checkArgs(addr);
    return addr.getHostName() + ":" + addr.getPort();
  }

  // FIXME - copied from DataNode.java

  /* Get the network location by running a script configured in conf */
  public static String getNetworkLoc(Configuration conf) throws IOException {
    String locScript = conf.get("dfs.network.script");
    if (locScript == null)
      return NetworkTopology.DEFAULT_RACK;

    LOG.info("Starting to run script to get datanode network location");
    Process p = Runtime.getRuntime().exec(locScript);
    StringBuffer networkLoc = new StringBuffer();
    final BufferedReader inR = new BufferedReader(new InputStreamReader(p
        .getInputStream()));
    final BufferedReader errR = new BufferedReader(new InputStreamReader(p
        .getErrorStream()));

    // read & log any error messages from the running script
    Thread errThread = new Thread() {
      @Override
      public void start() {
        try {
          String errLine = errR.readLine();
          while (errLine != null) {
            LOG.warn("Network script error: " + errLine);
            errLine = errR.readLine();
          }
        } catch (IOException e) {
          //
        }
      }
    };
    try {
      errThread.start();

      // fetch output from the process
      String line = inR.readLine();
      while (line != null) {
        networkLoc.append(line);
        line = inR.readLine();
      }
      try {
        // wait for the process to finish
        int returnVal = p.waitFor();
        // check the exit code
        if (returnVal != 0) {
          throw new IOException("Process exits with nonzero status: "
              + locScript);
        }
      } catch (InterruptedException e) {
        throw new IOException(e.getMessage());
      } finally {
        try {
          // make sure that the error thread exits
          errThread.join();
        } catch (InterruptedException je) {
          LOG.warn(StringUtils.stringifyException(je));
        }
      }
    } finally {
      // close in & error streams
      try {
        inR.close();
      } catch (IOException ine) {
        throw ine;
      } finally {
        errR.close();
      }
    }

    return networkLoc.toString();
  }
}
