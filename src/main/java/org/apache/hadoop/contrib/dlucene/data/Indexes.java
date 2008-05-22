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
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;

/**
 * A data structure that manages metadata about indexes using
 * {@link PrimaryIndexes}, {@link ReplicatedIndexes) and
 * {@link UncommittedIndexes}.
 */
public class Indexes extends ReplicatedIndexes {
  
  /** Logging. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.data.Indexes");

  /** a map of index names on to index locations */
  private PrimaryIndexes primaryIndexes = null;

  /** Uncommitted indexes */
  private UncommittedIndexes uncommittedIndexes = null;
  
  /** For generating random numbers. */
  private static final Random RANDOM = new Random();

  /**
   * Constructor.
   */
  public Indexes() {
    uncommittedIndexes = new UncommittedIndexes();
    primaryIndexes = new PrimaryIndexes();
  }
  
  /**
   * Commit an index.
   * 
   * @param indexName the index name
   * @return the corresponding IndexLocation
   */
  public IndexLocation commitIndex(String indexName) {
    IndexLocation location = uncommittedIndexes.get(indexName);
    if (location != null) {
      uncommittedIndexes.remove(location);
    }
    return location;
  }

  /**
   * Get the location of the primary index associated with an index.
   * 
   * @param indexName the index
   * @return the location of the primary index
   */
  public IndexVersion getPrimaryIndex(String indexName) {
    LOG.debug("Getting index " + indexName + " from " + primaryIndexes.toString());
    return primaryIndexes.get(indexName);
  }

  /**
   * Get the location of the uncommitted index associated with an index.
   * 
   * @param indexName the index
   * @return the location of the uncommitted index
   */
  public IndexLocation getUncommittedIndex(String indexName) {
    return uncommittedIndexes.get(indexName);
  }

  /**
   * Add an index.
   * 
   * @param location the index
   * @param rack the rack containing the DataNode with the index
   */
  public void add(IndexLocation location, String rack) {
    super.add(location, rack);
    primaryIndexes.add(location.getIndexVersion());
    if (location.getState() == IndexState.UNCOMMITTED) {
      uncommittedIndexes.add(location);
    } else {
      uncommittedIndexes.remove(location);
    }
  }

  /**
   * Remove an index.
   * 
   * @param location the index
   * @param rack the rack containing the DataNode with the index
   */
  public void remove(IndexLocation location, String rack) {
    super.remove(location, rack);
    primaryIndexes.remove(location.getIndexVersion());
    if (location.getState() == IndexState.UNCOMMITTED) {
      uncommittedIndexes.remove(location);
    }
  }
  
  /**
   * This is used to do load balancing when querying IndexVersion's. 
   * 
   * @param index the index
   * @return the replica location
   */
  public IndexLocation getIndexLocation(String index) {
    IndexVersion iv = getPrimaryIndex(index);
    Set<InetSocketAddress> replicas = getReplicaAddresses(iv);
    InetSocketAddress[] replicaArray = replicas
        .toArray(new InetSocketAddress[replicas.size()]);
    InetSocketAddress replicaPickedAtRandom = replicaArray[RANDOM
        .nextInt(replicas.size())];
    return new IndexLocation(replicaPickedAtRandom, iv, IndexState.LIVE);
  }
  
  /**
   * @param iv
   * @return
   */
  public IndexLocation getIndexLocation(IndexVersion iv) {
    Set<InetSocketAddress> replicas = getReplicaAddresses(iv);
    InetSocketAddress[] replicaArray = replicas
        .toArray(new InetSocketAddress[replicas.size()]);
    InetSocketAddress replicaPickedAtRandom = replicaArray[RANDOM
        .nextInt(replicas.size())];
    return new IndexLocation(replicaPickedAtRandom, iv, IndexState.LIVE);
  }
  
}


