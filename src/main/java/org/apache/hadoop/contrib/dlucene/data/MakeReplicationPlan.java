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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.network.Network;

/**
 * This class builds the replication plan.
 */
public class MakeReplicationPlan {

  /** Used for generating random number. */
  private final static Random r = new Random();

  /** A map of socket addresses on to metadata about DataNodes. */
  private NameNodeData dataNodes = null;

  /** The minimum number of racks an index should be replicated across. */
  private boolean secondReplicaOnAnotherRack;

  /** Minimum number of replicas of an index. */
  private int minNumReplicas;

  /**
   * The maximum number of replication tasks that can be assigned to a specific
   * node in a heartbeat.
   */
  private int maxNumReplicaTasks;

  /** Datastructure storing the replication plan. */
  private ReplicationPlan replicationPlan = null;

  /**
   * Constructor.
   */
  public MakeReplicationPlan(NameNodeData nameNodeData) {
    this.dataNodes = nameNodeData;
    Configuration conf = nameNodeData.getConfiguration();
    // avTotalCapacityFree = nameNodeData.getTotalCapacityRemaining() /
    // nameNodeData.getTotalCapacity();
    this.minNumReplicas = conf.getInt(Constants.REPLICAS_NAME,
        Constants.MINIMUM_NUMBER_OF_REPLICAS);
    this.secondReplicaOnAnotherRack = conf.getBoolean(
        Constants.REPLICAS_RACKS_NAME, true);
    this.maxNumReplicaTasks = conf.getInt(
        Constants.MAXIMUM_NUMBER_OF_REPLICAS_PER_HEARTBEAT_NAME,
        Constants.MAXIMUM_NUMBER_OF_REPLICAS_PER_HEARTBEAT);
  }

  /**
   * Create the replication plan
   * 
   * @return
   * @throws IOException
   */
  public Map<InetSocketAddress, IndexLocation[]> createReplicationPlan() {
    replicationPlan = new ReplicationPlan();
    if (dataNodes.getTotalCapacity() > 0) {
      Indexes indexes = dataNodes.getIndexes();
      for (IndexVersion version : indexes.getIndexes()) {
        Set<InetSocketAddress> addresses = indexes.getReplicaAddresses(version);
        if (addresses.size() <= minNumReplicas) {
          Set<String> rackSet = indexes.getReplicaRacks(version);
          String[] racks = rackSet.toArray(new String[rackSet.size()]);
          makeNewReplica(version, racks, addresses);
        }
      }
    }
    return replicationPlan.get();
  }

  private void makeNewReplica(IndexVersion version, String[] racks,
      Set<InetSocketAddress> addresses) {
    boolean founddatanode = false;
    int rackIndex = r.nextInt(racks.length);
    String networkLocation = racks[rackIndex];
    Iterator<InetSocketAddress> addrIter = chooseRandomDataNode(
        networkLocation, racks.length);
    while (!founddatanode && addrIter.hasNext()) {
      // does the datanode already have a copy?
      InetSocketAddress addr = addrIter.next();
      if (!addresses.contains(addr)) {
        // no
        DataNodeStatus status = dataNodes.getStatus(addr);
        float avNodeCapacityFree = status.getPercentageCapacityRemaining();
        if (dataNodes.doesNodeHaveSufficientFreeSpace(avNodeCapacityFree)) {
          int numExistingReplicaTasks = dataNodes.getStatus(addr)
              .getReplicationTasks().size();
          if (replicationPlan.numberOfTasks(addr) + numExistingReplicaTasks < maxNumReplicaTasks) {
            founddatanode = true;
            InetSocketAddress primary = addresses.iterator().next();
            replicationPlan.add(version, addr, primary);
          }
        }
      }
    }
  }

  private Iterator<InetSocketAddress> chooseRandomDataNode(
      String networkLocation, int numRacks) {
    Network network = dataNodes.getNetwork();
    if (numRacks == 1 && secondReplicaOnAnotherRack) {
      return network.getRandomNode("~" + networkLocation).iterator();
    }
    return network.getRandomNode(networkLocation).iterator();
  }
}
