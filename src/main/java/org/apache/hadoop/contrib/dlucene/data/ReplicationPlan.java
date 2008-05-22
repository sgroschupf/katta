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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;

/**
 * Datastructure storing the replication plan.
 */
class ReplicationPlan {

  /** Temporary data structure for the replication plan. */
  private Map<InetSocketAddress, List<IndexLocation>> plan = null;

  /**
   * Constructor.
   */
  ReplicationPlan() {
    plan = new HashMap<InetSocketAddress, List<IndexLocation>>();
  }

  /**
   * Add a new replication task.
   * 
   * @param version the version of the index to be replicated
   * @param newAddress the address to copy to
   * @param primaryAddress the address to copy from
   */
  void add(IndexVersion version, InetSocketAddress newAddress,
      InetSocketAddress primaryAddress) {
    List<IndexLocation> tasks = plan.containsKey(newAddress) ? plan
	.get(newAddress) : new ArrayList<IndexLocation>();
    IndexLocation location = new IndexLocation(primaryAddress, version,
	IndexState.REPLICATING);
    tasks.add(location);
    plan.put(newAddress, tasks);
  }

  /**
   * Get the replication plan.
   * 
   * @return the replication plan
   */
  Map<InetSocketAddress, IndexLocation[]> get() {
    Map<InetSocketAddress, IndexLocation[]> repPlan = new HashMap<InetSocketAddress, IndexLocation[]>();
    for (InetSocketAddress addr : plan.keySet()) {
      List<IndexLocation> j = plan.get(addr);
      IndexLocation[] i = j.toArray(new IndexLocation[j.size()]);
      repPlan.put(addr, i);
    }
    return repPlan;
  }

  /**
   * The number of replication tasks assigned to a particular DataNode.
   * 
   * @param addr the DataNode.
   * @return the number of tasks
   */
  int numberOfTasks(InetSocketAddress addr) {
    return plan.containsKey(addr) ? plan.get(addr).size() : 0;
  }
}
