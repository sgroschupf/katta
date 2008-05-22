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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
;

public class Lease implements Writable {
  
  /** Log file. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.Lease");
  
  /** The index owning the lease. */
  private IndexVersion index;
  
  /** The address of the machine owning the lease. */
  private InetSocketAddress addr;
  
  /** The time the lease was last updated. */
  private long lastUpdated;
  
  /** How long leases lasts */
  private static long leaseLength;
  
  /**
   * Constructor.
   * 
   * @param index the index that the lease applies to.
   * @param leaseLength the length of the lease.
   */
  public Lease(IndexLocation index, long _leaseLength) {
    this.index = index.getIndexVersion();
    this.lastUpdated = System.currentTimeMillis();
    this.addr = index.getAddress();
    LOG.info(addr.toString());
    leaseLength = _leaseLength;
  }
  
  /**
   * Constructor.
   */
  private Lease() {
    // 
  }
  
  /**
   * Is this lease valid?
   * 
   * @return is the lease valid?
   */
  public boolean isValid() {
    return lastUpdated + leaseLength >= System.currentTimeMillis() ;
  }
  
  /**
   * Renew the lease.
   */
  public void renew() {
    this.lastUpdated = System.currentTimeMillis();
  }
  
  /**
   * Deserialize the Lease object.
   * 
   * @param in the input stream
   * @return the Lease object
   * @throws IOException
   */
  public static Lease read(DataInput in) throws IOException {
    Lease lease = new Lease();
    lease.readFields(in);
    return lease;
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
    out.writeLong(lastUpdated);
    index.write(out);
    Text.writeString(out, Network.convertInetSocketAddress(getAddress()));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    lastUpdated = in.readLong();
    index = IndexVersion.read(in);
    this.addr = NetUtils.createSocketAddr(Text.readString(in));
  }
  
  /**
   * @return the index.
   */
  public IndexVersion getIndex() {
    return index;
  }

  /**
   * @return the address.
   */
  public InetSocketAddress getAddress() {
    return addr;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((addr == null) ? 0 : addr.hashCode());
    result = prime * result + ((index == null) ? 0 : index.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final Lease other = (Lease) obj;
    if (addr == null) {
      if (other.addr != null)
        return false;
    } else if (!addr.equals(other.addr))
      return false;
    if (index == null) {
      if (other.index != null)
        return false;
    } else if (!index.equals(other.index))
      return false;
    return true;
  }
}
