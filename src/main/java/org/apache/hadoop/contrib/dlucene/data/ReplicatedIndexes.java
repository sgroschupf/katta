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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexVersion;

/**
 * A data structure mapping {@link IndexVersion} on to {@link Replicas}.
 */
class ReplicatedIndexes {

  /** a map of index versions onto replication locations. */
  private Map<IndexVersion, Replicas> indexes = null;

  /**
   * Constructor.
   */
  ReplicatedIndexes() {
    indexes = new HashMap<IndexVersion, Replicas>();
  }

  /**
   * Add an index.
   * 
   * @param location the location of the index
   * @param rack the rack containing the datanode storing the index
   */
  void add(IndexLocation location, String rack) {
    IndexVersion version = location.getIndexVersion();
    Replicas data = indexes.containsKey(version) ? indexes.get(version)
        : new Replicas();
    data.add(location.getAddress(), rack);
    indexes.put(version, data);
  }

  /**
   * Remove an index.
   * 
   * @param location the location of the index
   * @param rack the rack containing the datanode storing the index
   */
  void remove(IndexLocation location, String rack) {
    IndexVersion version = location.getIndexVersion();
    Replicas data = indexes.get(version);
    data.remove(location.getAddress(), rack);
    if (data.locationOfReplicas.size() > 0) {
      indexes.put(version, data);
    } else {
      indexes.remove(version);
    }
  }

  /**
   * Get all the indexes as a set.
   * 
   * @return all the unique IndexVersions
   */
  public Set<IndexVersion> getIndexes() {
    return indexes.keySet();
  }

  /**
   * Get the addresses of the replicas of a specific index.
   * 
   * @param index the index
   * @return the addresses
   */
  public Set<InetSocketAddress> getReplicaAddresses(IndexVersion index) {
    return indexes.containsKey(index) ? indexes.get(index).locationOfReplicas
        : null;
  }

  /**
   * Get replica racks used by the replicas of a specific index.
   * 
   * @param index the index
   * @return the racks
   */
  public Set<String> getReplicaRacks(IndexVersion index) {
    return indexes.containsKey(index) ? indexes.get(index).getRacks() : null;
  }

  /**
   * Inner class storing the location of all the replicas of an index.
   */
  class Replicas {

    /** The address of each replica. */
    private Set<InetSocketAddress> locationOfReplicas = null;

    /** The number of replicas on each rack. */
    private Map<String, Integer> numberOfReplicasOnRack = null;

    /**
     * Constructor.
     */
    private Replicas() {
      locationOfReplicas = new HashSet<InetSocketAddress>();
      numberOfReplicasOnRack = new HashMap<String, Integer>();
    }

    /**
     * Add a replica location.
     * 
     * @param location the address of the datanode with the replica
     * @param rack the rack containing the datanode
     */
    private void add(InetSocketAddress location, String rack) {
      locationOfReplicas.add(location);
      Integer i = numberOfReplicasOnRack.containsKey(rack) 
        ? numberOfReplicasOnRack.get(rack)
          : Integer.valueOf(0);
      i++;
      numberOfReplicasOnRack.put(rack, i);
    }

    /**
     * Remove a replica location.
     * 
     * @param location the address of the datanode with the replica
     * @param rack the rack containing the datanode
     */
    private void remove(InetSocketAddress location, String rack) {
      if (!locationOfReplicas.contains(location)) {
        throw new IllegalArgumentException("cannot remove " + location
            + " as has not been stored");
      }
      this.locationOfReplicas.remove(location);
      Integer i = numberOfReplicasOnRack.get(rack);
      i--;
      if (i > 0) {
        numberOfReplicasOnRack.put(rack, i);
      } else {
        numberOfReplicasOnRack.remove(rack);
      }
    }

    /**
     * Get the racks this index is stored on.
     * 
     * @return the racks
     */
    private Set<String> getRacks() {
      return numberOfReplicasOnRack.keySet();
    }
  }
}
