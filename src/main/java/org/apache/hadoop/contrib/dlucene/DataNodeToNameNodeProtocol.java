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

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/** datanode to namenode protocol. */
public interface DataNodeToNameNodeProtocol extends VersionedProtocol {

  /** The version of the protocol. */
  long VERSION_ID = 1L;

  /**
   * Send a heartbeat message to the namenode.
   * 
   * @param status The status of the datanode.
   * @param searchableIndexes The indexes on the datanode.
   * @param leases The leases owned by the datanode.
   * @return the indexes to replicate
   * @throws IOExceptiuon
   */
  HeartbeatResponse heartbeat(DataNodeStatus status,
      IndexLocation[] searchableIndexes, Lease[] leases) 
  throws IOException;

  /**
   * Get a lease to be the primary replica for a specific index.
   * 
   * @param index The index requiring the lease.
   * @return the lease.
   * @throws LeaseException
   */
  Lease getLease(IndexLocation index) throws IOException ;

  /**
   * Relinquish a lease.
   * 
   * @param lease the lease.
   * @return was operation successful
   */
  public boolean relinquishLease(Lease lease) throws IOException ;
}
