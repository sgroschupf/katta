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
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.Lease;
import org.apache.hadoop.contrib.dlucene.LeaseException;

public class NameNodeLeaseManager {
  // FIXME - this breaks no non replicated data design, if name node fails then
  // will this still work? need to think through

  /** Concurrency control. */
  private final Lock lock = new ReentrantLock();

  /** Log file for this node. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.data.NameNodeLeaseManager");

  /** How long do leases last before timing out? */
  private static long leaseLength = 0;

  /** The leases issued by the NameNode */
  private Map<String, Lease> leases = new HashMap<String, Lease>();

  /**
   * Constructor.
   * 
   * @param leaseLength the length a lease lasts.
   */
  public NameNodeLeaseManager(long leaseLength) {
    NameNodeLeaseManager.leaseLength = leaseLength;
  }

  /**
   * Get a lease.
   * 
   * @param index the index that needs the lease.
   * @param the address of the node that needs the lease.
   * @return The lease.
   * @throws LeaseException Thrown if a lease is not available.
   */
  public Lease getLease(IndexVersion index, InetSocketAddress addr)
      throws IOException {
    Lease lease = null;
    String name = index.getName();
    LOG.debug("Getting lease for " + index + " with address " + addr);
    lock.lock();
    try {
      if (!leases.containsKey(name)) {
        LOG.debug("Datanode " + addr
            + " successfully acquired lease for index " + index);
        IndexLocation il = new IndexLocation(addr, index, IndexState.UNKNOWN);
        lease = new Lease(il, leaseLength);
        leases.put(name, lease);
      } else {
        // someone already has the lease
        StringBuffer msg = new StringBuffer();
        Lease activeLease = leases.get(name);
        msg.append("Datanode " + addr + " failed to acquire lease for index "
            + index + " becase Datanode " + activeLease.getAddress()
            + " has the lease");
        LOG.error(msg.toString());
        throw new IOException(msg.toString());
      }
    } finally {
      lock.unlock();
    }
    return lease;
  }

  /**
   * Relinquish the lease.
   * 
   * @param lease The lease to relinquish.
   * @return Was this successful?
   * @throws LeaseException
   */
  public boolean relinquishLease(Lease lease) throws IOException {
    boolean success = false;
    if (lease != null) {
      IndexVersion iv = lease.getIndex();
      String name = iv.getName();
      lock.lock();
      try {
        if (leases.containsKey(name)) {
          if (leases.get(name).equals(lease)) {
            LOG.debug("Datanode " + lease.getAddress()
                + " successfully relinquished lease for index " + iv);
            leases.remove(name);
            success = true;
          }
        }
      } finally {
        lock.unlock();
      }
    }
    if (!success) {
      // no such lease
      StringBuffer msg = new StringBuffer();
      if (lease != null) {
        msg.append("Datanode " + lease.getAddress()
            + " attempted to relinquish a lease and failed");
        throw new IOException(msg.toString());
      } else
        throw new IOException("Lease was null");
    }
    return success;
  }

  /**
   * Check that all the leases are valid. If not, remove them.
   */
  public void checkLeases() {
    lock.lock();
    try {
      for (String name : leases.keySet()) {
        Lease lease = leases.get(name);
        if (!lease.isValid()) {
          LOG.error("Datanode " + lease.getAddress() + " has a lease on index "
              + lease.getIndex() + " but it has expired");
          leases.remove(name);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update the expires on these leases.
   * 
   * @param leaseUpdate the leases.
   * @return The updated leases.
   */
  public Lease[] updateLeases(Lease[] leaseUpdate) {
    if (leaseUpdate != null) {
      lock.lock();
      try {
        for (int i = 0; i < leaseUpdate.length; i++) {
          Lease lease = leaseUpdate[i];
          if (lease != null) {
            IndexVersion iv = lease.getIndex();
            String name = iv.getName();
            if (leases.containsKey(name)) {
              Lease storedLease = leases.get(name);
              if (lease.equals(storedLease)) {
                lease.renew();
              } else {
                // error - more than one node has this lease
                LOG.error("Error - multiple datanodes have lease for index "
                    + iv);
                lease = null;
              }
            } else {
              LOG.error("Error - datanode " + lease.getAddress()
                  + " has a lease for index " + iv
                  + " but the NameNode has no record of it");
              // error - never issued this lease
              lease = null;
            }
            leases.put(name, lease);
            leaseUpdate[i] = lease;
          }
        }
      } finally {
        lock.unlock();
      }
    }
    return leaseUpdate;
  }
}
