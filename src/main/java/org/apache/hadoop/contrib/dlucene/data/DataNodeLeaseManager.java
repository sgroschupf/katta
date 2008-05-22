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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.DataNodeToNameNodeProtocol;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.Lease;
import org.apache.hadoop.contrib.dlucene.LeaseException;

/**
 * A class for datanodes to manage leases.
 */
public class DataNodeLeaseManager {

  /** Log file. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.data.DataNodeLeaseManager");

  /** The leases owned by this datanode. */
  private HashMap<String, Lease> theLeases = new HashMap<String, Lease>();

  /** Interface to access namenode. */
  private DataNodeToNameNodeProtocol namenode = null;

  /** The address of this datanode. */
  private InetSocketAddress addr = null;

  DataNodeLeaseManager(DataNodeToNameNodeProtocol namenode,
      InetSocketAddress addr) {
    this.namenode = namenode;
    this.addr = addr;
  }

  /**
   * @return All the leases owned by this datanode.
   */
  public Lease[] getLeases() {
    return theLeases.values().toArray(new Lease[theLeases.size()]);
  }

  /**
   * Get a lease for a specific index version.
   * 
   * @param iv The index version.
   * @return The lease.
   * @throws LeaseException thrown if the lease cannot be obtained.
   */
  boolean getLease(IndexVersion iv) throws LeaseException {
    try {
      String name = iv.getName();
      if (theLeases.containsKey(name)) {
        Lease lease = theLeases.get(name);
        return lease.isValid();
      }
      LOG.debug("Requesting lease from namenode for " + iv + " with address " + addr);
      IndexLocation il = new IndexLocation(addr, iv, IndexState.UNKNOWN);
      Lease lease = namenode.getLease(il);
      if (lease != null) {
        theLeases.put(name, lease);
        return lease.isValid();
      }
    } catch (IOException io) {
      throw new LeaseException(io.getMessage());
    }
    LOG.error("Could not get lease for " + iv + " from namenode");
    throw new LeaseException("Could not get lease for " + iv);
  }

  /**
   * Relinquish a lease when an index is committed.
   * 
   * @param iv the index version.
   * @throws LeaseException Thrown if there was a problem relinquishing the lease.
   */
  void relinquishLease(IndexVersion iv) throws LeaseException {
    String name = iv.getName();
    if (theLeases.containsKey(name)) {
      try {
      namenode.relinquishLease(theLeases.get(name));
      theLeases.remove(name);
      } catch (IOException io) {
        io.printStackTrace();
        throw new LeaseException(io.getMessage());
      }
    } else {
      throw new LeaseException("Datanode " + addr
          + " could not relinquish lease for " + iv);
    }
  }
}
