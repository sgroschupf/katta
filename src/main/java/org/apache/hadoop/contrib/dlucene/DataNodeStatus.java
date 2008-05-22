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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * Stores information about the status of a particular DataNode.
 */
public class DataNodeStatus implements Writable {

  /** Logging . */
  protected static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.DataNodeStatusInformation");

  /** The total capacity available for Lucene indexes. */
  private long capacity = 0;

  /** The capacity already used for Lucene indexex. */
  private long capacityUsed = 0;

  /** The capacity remaining for Lucene indexes. */
  private long capacityRemaining = 0;

  /** Used to call the UNIX command DF. */
  private DF usage;

  /** Used to call the UNIX command DU to determine disk useage. */
  private DU dfsUsage;

  /** The configuration of this data node. */
  private DataNodeConfiguration dataNodeConfiguration = null;

  /** A set of any ongoing replication tasks. */
  Set<IndexLocation> replicationTasks = null;

  /** Controls shared access to data structure. */
  private final Lock lock = new ReentrantLock();

  /**
   * Get the next outstanding replication task.
   * 
   * @return the location of the next index to replicate
   */
  IndexLocation getNextReplicationTask() {
    IndexLocation location = null;
    try {
      lock.lock();
      location = replicationTasks.iterator().next();
    } finally {
      lock.unlock();
    }
    if (replicationTasks.size() > 0) {
      return location;
    } 
    return null;
  }

  /**
   * Update information on disk usage.
   * 
   * @throws IOException
   */
  void updateUsage() {
    try {
      this.capacity = usage.getCapacity();
      this.capacityUsed = dfsUsage.getUsed();
      this.capacityRemaining = usage.getAvailable();
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  /**
   * Constructor.
   * 
   * @param dataNodeConfiguration the configuration of this DataNode
   * @param configuration the Hadoop configuration
   * @throws IOException
   */
  DataNodeStatus(DataNodeConfiguration dataNodeConfiguration,
      Configuration configuration) throws IOException {
    this();
    this.dataNodeConfiguration = dataNodeConfiguration;
    this.usage = new DF(dataNodeConfiguration.getRootDir(), configuration);
    this.dfsUsage = new DU(dataNodeConfiguration.getRootDir(), configuration);
    if (!dataNodeConfiguration.getRootDir().exists()) {
      dataNodeConfiguration.getRootDir().mkdirs();
    }
    updateUsage();
  }

  /**
   * Constructor.
   * 
   * @throws IOException
   */
  public DataNodeStatus() {
    replicationTasks = new HashSet<IndexLocation>();
  }

  /**
   * Deserialize this object.
   * 
   * @param in the input stream
   * @return the object
   * @throws IOException
   */
  public static DataNodeStatus read(DataInput in) throws IOException {
    DataNodeStatus dnc = new DataNodeStatus();
    dnc.readFields(in);
    return dnc;
  }

  /**
   * @return the capacity
   */
  public long getCapacity() {
    return capacity;
  }

  /**
   * @return the capacity used
   */
  public long getCapacityUsed() {
    return capacityUsed;
  }

  /**
   * @return the capacity remaining
   */
  public long getCapacityRemaining() {
    return capacityRemaining;
  }

  /**
   * Get the address of this DataNode.
   * 
   * @return the address
   */
  public InetSocketAddress getAddress() {
    return dataNodeConfiguration.getAddress();
  }

  /**
   * @return Get the rack this node is on
   */
  public String getRack() {
    return dataNodeConfiguration.getRack();
  }

  /**
   * Add a replication task.
   * 
   * @param task the location of the index to replicate
   */
  void addReplicationTask(IndexLocation task) {
    try {
      lock.lock();
      replicationTasks.add(task);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Mark a replication task as finished.
   * 
   * @param task the location of the index to replicate
   */
  void removeReplicationTask(IndexLocation task) {
    try {
      lock.lock();
      replicationTasks.remove(task);
    } finally {
      lock.unlock();
    }
  }

  /**
   * The number of outstanding replication tasks.
   * 
   * @return the number of outstanding replication tasks
   */
  public Set<IndexLocation> getReplicationTasks() {
    return replicationTasks;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    Utils.checkArgs(out);
    dataNodeConfiguration.write(out);
    out.writeLong(capacity);
    out.writeLong(capacityUsed);
    out.writeLong(capacityRemaining);
    out.writeInt(replicationTasks.size());
    for (IndexLocation location : replicationTasks) {
      location.write(out);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    dataNodeConfiguration = DataNodeConfiguration.read(in);
    this.capacity = in.readLong();
    this.capacityUsed = in.readLong();
    this.capacityRemaining = in.readLong();
    int numberOfReplicationTasks = in.readInt();
    for (int i = 0; i < numberOfReplicationTasks; i++) {
      replicationTasks.add(IndexLocation.read(in));
    }
  }

  /**
   * @return get the capacity of free space remaining on this node
   */
  public float getPercentageCapacityRemaining() {
    return (float) getCapacityRemaining() / (float) getCapacity();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append("Capacity: " + capacity + "\n");
    result.append("CapacityUsed: " + capacityUsed + "\n");
    result.append("CapacityRemaining: " + capacityRemaining + "\n");
    return result.toString();
  }
}
