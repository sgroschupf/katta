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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.DataNode;
import org.apache.hadoop.contrib.dlucene.DataNodeStatus;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.NameNode;
import org.apache.hadoop.contrib.dlucene.Utils;

/**
 * Structure that keeps track of all {@link DataNode}s in the cluster.
 */
class DataNodes {
  /** A map of socket addresses on to metadata about DataNodes. */
  private Map<InetSocketAddress, DataNodeDescription> datanodesBySocket = null;

  /**
   * Constructor.
   */
  DataNodes() {
    datanodesBySocket = new HashMap<InetSocketAddress, DataNodeDescription>();
  }

  /**
   * Get the addresses of all the DataNodes in the cluster.
   * 
   * @return the DataNodes
   */
  Set<InetSocketAddress> getDataNodes() {
    return datanodesBySocket.keySet();
  }

  /**
   * Get the status of a specific DataNode.
   * 
   * @param addr the address of the DataNode
   * @return the status
   */
  DataNodeStatus getStatus(InetSocketAddress addr) {
    Utils.checkArgs(addr);
    return datanodesBySocket.containsKey(addr) 
      ? datanodesBySocket.get(addr).status
      : null;
  }

  /**
   * Get all the indexes managed by a specific DataNode.
   * 
   * @param addr the address of the DataNode
   * @return the indexes
   */
  IndexLocation[] getIndexes(InetSocketAddress addr) {
    Utils.checkArgs(addr);
    return datanodesBySocket.containsKey(addr) 
      ? datanodesBySocket.get(addr).indexes
      : null;
  }

  /**
   * Get the time of the last heartbeat from a specific DataNode.
   * 
   * @param addr the address of the DataNode
   * @return the time
   */
  long getLastHeartBeat(InetSocketAddress addr) {
    Utils.checkArgs(addr);
    return datanodesBySocket.containsKey(addr) 
      ? datanodesBySocket.get(addr).lastHeartBeat
      : 0;
  }

  /**
   * Remove a specific DataNode.
   * 
   * @param addr the address
   */
  void remove(InetSocketAddress addr) {
    datanodesBySocket.remove(addr);
  }

  /**
   * Add a specific DataNode.
   * 
   * @param status the status of the DataNode
   * @param indexes the indexes managed by the DataNode
   */
  void add(DataNodeStatus status, IndexLocation[] indexes) {
    datanodesBySocket.put(status.getAddress(), new DataNodeDescription(status,
	indexes));
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append(super.toString());
    return result.toString();
  }

  /**
   * Inner class representing a {@link DataNode} held by a {@link NameNode}.
   */
  private final class DataNodeDescription {
    /** The status of the DataNode. */
    private DataNodeStatus status = null;

    /** The indexes managed by the DataNode. */
    private IndexLocation[] indexes = null;

    /** Last time the DataNode sent a heartbeat. */
    private long lastHeartBeat = 0;

    /**
     * Constructor.
     * 
     * @param status the status of the DataNode
     * @param indexes the indexes managed by the DataNode
     */
    private DataNodeDescription(DataNodeStatus status, IndexLocation[] indexes) {
      this.status = status;
      this.indexes = indexes;
      this.lastHeartBeat = System.currentTimeMillis();
    }
  }
}
