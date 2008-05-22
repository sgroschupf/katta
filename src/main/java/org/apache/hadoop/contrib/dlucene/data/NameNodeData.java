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
package org.apache.hadoop.contrib.dlucene.data;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

/**
 * The data structure used by the NameNode.
 */
public class NameNodeData extends DataNodes {
  
  /** Log file for this namenode. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.NameNodeData");

  /**
   * How far a datanode can be away from average percentage available capacity
   * to be used for a replication task.
   */
  private float saturationFactor;

  /**
   * The maximum number of replication tasks that can be assigned to a specific
   * node in a heartbeat.
   */
  private int maximumNumberOfReplicationTasks;

  /** All the indexes currently in this cluster. */
  private Set<IndexLocation> allIndexes = null;

  /** A data structure managing indexes. */
  private Indexes indexes = null;

  /** The total disk capacity for indexes across the cluster. */
  private long totalCapacity = 0;

  /** The total free disk capacity for indexes across the cluster. */
  private long totalCapacityRemaining = 0;

  /** the total number of datanodes. */
  private long numberOfDatanodes = 0;

  /** The heart beat interval, determined from the configuration file. */
  private long heartBeatInterval = Constants.HEARTBEAT_INTERVAL_VALUE;

  /** The Hadoop configuration. */
  private Configuration configuration = null;

  /** Information about the network. */
  private Network network = null;

  /**
   * Constructor.
   * 
   * @param configuration the Hadoop configuration
   * @param addr
   * @throws Exception
   */
  public NameNodeData(Configuration configuration) throws Exception {
    this.configuration = configuration;
    // set the heart beat interval
    this.heartBeatInterval = 1000L * configuration.getLong(
        Constants.HEARTBEAT_INTERVAL_NAME, Constants.HEARTBEAT_INTERVAL_VALUE);
    this.saturationFactor = configuration.getFloat(
        Constants.SATURATION_FACTOR_NAME, Constants.SATURATION_FACTOR);
    indexes = new Indexes();
    allIndexes = new HashSet<IndexLocation>();
    network = new Network();
  }

  /**
   * Get all currently searchable indexes.
   *
   * @return their locations
   */
  public IndexLocation[] getSearchableIndexes() {
    // FIXME this is going to be slow?
    Set<IndexLocation> result = new HashSet<IndexLocation>();
    result.addAll(allIndexes);
    for (IndexLocation l : result) {
      if (!l.getState().equals(IndexState.LIVE)) {
        result.remove(l);
      }
    }
    return result.toArray(new IndexLocation[result.size()]);
  }

  /**
   * Get a datanode at random to store a new Index.
   *
   * @return the datanode
   */
  public String getRandomDataNode() {
    Collection<InetSocketAddress> addr = null;
    try {
      addr = getNetwork().getRandomNode(NodeBase.ROOT);

      for (InetSocketAddress a : addr) {
        DataNodeStatus status = super.getStatus(a);
        float avCapacityRemaining = status.getPercentageCapacityRemaining();
        if (doesNodeHaveSufficientFreeSpace(avCapacityRemaining)) {
          String s = Network.convertInetSocketAddress(a);
          LOG.info("Returning " + s);
          return s;
        }
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }

  public boolean doesNodeHaveSufficientFreeSpace(float avNodeCapacityFree) {
    float upperBound = (float) getTotalCapacityRemaining()
        / (float) getTotalCapacity() * saturationFactor;
    return avNodeCapacityFree > upperBound;
  }

  /**
   * Store the status information from the datanode.
   *
   * @param status
   */
  public void add(DataNodeStatus status, IndexLocation[] indexesToAdd) {
    Utils.checkArgs(status, indexesToAdd);
    InetSocketAddress addr = status.getAddress();
    if (super.getDataNodes().contains(addr)) {
      // this is not a new datanode
      DataNodeStatus oldStatus = super.getStatus(addr);
      totalCapacity -= oldStatus.getCapacity();
      totalCapacityRemaining -= oldStatus.getCapacityRemaining();
      numberOfDatanodes--;
    }
    totalCapacity += status.getCapacity();
    totalCapacityRemaining += status.getCapacityRemaining();
    numberOfDatanodes++;
    network.add(status);
    super.add(status, indexesToAdd);
    for (IndexLocation location : indexesToAdd) {
      indexes.add(location, status.getRack());
      if (location.getState() == IndexState.LIVE) {
        allIndexes.add(location);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append(super.toString());
    result.append("maximumNumberOfReplicationTasks: "
        + maximumNumberOfReplicationTasks + "\n");
    result.append("heartBeatInterval: " + heartBeatInterval + "\n");
    result.append("locations: " + indexes.toString() + "\n");
    result.append("network: " + network.toString() + "\n");
    return result.toString();
  }

  /**
   * Perform the heartbeat operation.
   */
  public Set<InetSocketAddress> doFailureDetect() {
    Set<InetSocketAddress> failures = new HashSet<InetSocketAddress>();
    for (InetSocketAddress addr : super.getDataNodes()) {
      if (System.currentTimeMillis() - super.getLastHeartBeat(addr) > heartBeatInterval * 2) {
        failures.add(addr);
      }
    }
    for (InetSocketAddress addr : failures) {
      remove(addr);
    }
    return failures;
  }

  void remove(InetSocketAddress addr) {
    DataNodeStatus status = super.getStatus(addr);
    for (IndexLocation location : super.getIndexes(addr)) {
      indexes.remove(location, status.getRack());
      allIndexes.remove(location);
    }
    network.remove(getStatus(addr));
    super.remove(addr);
  }

  /**
   * @return the indexes
   */
  Indexes getIndexes() {
    return indexes;
  }

  /**
   * @return the totalCapacity
   */
  long getTotalCapacity() {
    return totalCapacity;
  }

  /**
   * @return the totalCapacityRemaining
   */
  long getTotalCapacityRemaining() {
    return totalCapacityRemaining;
  }

  /**
   * @return the configuration
   */
  Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Get the Network datastructure.
   *
   * @return the network datastructure
   */
  Network getNetwork() {
    return network;
  }
}
