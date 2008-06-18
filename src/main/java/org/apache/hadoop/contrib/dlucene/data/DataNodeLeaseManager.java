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

import net.sf.katta.util.KattaException;
import net.sf.katta.zk.ZKClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.Lease;

/**
 * A class for datanodes to manage leases.
 */
public class DataNodeLeaseManager {

  /** Log file. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.data.DataNodeLeaseManager");

  /** The leases owned by this datanode. */
  private HashMap<String, Lease> theLeases = new HashMap<String, Lease>();

  /** The address of this datanode. */
  private InetSocketAddress addr = null;

  private ZKClient _client = null;

  /** How long do leases last before timing out? */
  private static long _leaseLength = 0;

  DataNodeLeaseManager(final ZKClient client, InetSocketAddress addr,
      long leaseLength) {
    _leaseLength = leaseLength;
    _client = client;
    this.addr = addr;
  }

  /**
   * @return All the leases owned by this datanode.
   */
  public Lease[] getLeases() {
    return theLeases.values().toArray(new Lease[theLeases.size()]);
  }
  
  public void updateLeases() throws KattaException {
    for (Lease l : getLeases()) {
      l.renew();
      final String path = getPath(l);
      _client.writeData(Constants.zkLeasePath
          + Constants.zkSeparator + l.getIndex().getName()
          + Constants.zkSeparator + l.getIndex().getVersion(), l);
    }
  }

  /**
   * Get a lease for a specific index version.
   * 
   * @param iv The index version.
   * @return The lease.
   * @throws LeaseException thrown if the lease cannot be obtained.
   */
  boolean getLease(IndexVersion iv) throws KattaException {
    String name = iv.getName();
    if (theLeases.containsKey(name)) {
      Lease lease = theLeases.get(name);
      return lease.isValid();
    }
    LOG.debug("Requesting lease from zookeeper for " + iv + " with address "
        + addr);
    IndexLocation il = new IndexLocation(addr, iv, IndexState.UNKNOWN);
    Lease lease = new Lease(il, _leaseLength);
    final String path = getPath(iv);
    try {
      _client.readData(path, lease);
    } catch (KattaException ke) {
      // exception should be thrown, otherwise lease is already in use
      // FIXME should not use an exception with these semantics
      _client.writeData(path, lease);
      theLeases.put(name, lease);
      return lease.isValid();
    }
    LOG.error("Could not get lease for " + iv + " from namenode");
    throw new KattaException("Could not get lease for " + iv, new Exception());
  }

  /**
   * Relinquish a lease when an index is committed.
   * 
   * @param iv the index version.
   * @throws LeaseException Thrown if there was a problem relinquishing the lease.
   */
  void relinquishLease(IndexVersion iv) throws KattaException {
    String name = iv.getName();
    if (theLeases.containsKey(name)) {
      Lease lease = theLeases.get(name);
      final String path = getPath(iv);
      _client.readData(path, lease); // should throw an exception here if the lease does not exist
      _client.delete(path);
      theLeases.remove(name);
    } else {
      throw new KattaException("Datanode " + addr
          + " could not relinquish lease for " + iv, new Exception());
    }
  }

  private String getPath(Lease l) {
    IndexVersion iv = l.getIndex();
    return Constants.zkLeasePath + Constants.zkSeparator + iv.getName()
        + Constants.zkSeparator + iv.getVersion();
  }
}
