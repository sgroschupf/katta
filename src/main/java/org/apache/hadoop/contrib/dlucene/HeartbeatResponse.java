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
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class HeartbeatResponse implements Writable {
  /** The indexes to be replicated. */
  private IndexLocation[] replicationRequests = null;

  /** The leases owned by this node. */
  private Lease[] updatedLeases = null;

  /**
   * Constructor
   * 
   * @param il an array of indexes to be replicated by this datanode.
   * @param l an array of leases owned by this datanode.
   */
  HeartbeatResponse(IndexLocation[] il, Lease[] l) {
    replicationRequests = il;
    updatedLeases = l;
  }

  /**
   * Constructor.
   */
  private HeartbeatResponse() {
  }

  /**
   * Deserialize the HeartbeatResponse object.
   * 
   * @param in the input stream
   * @return the HeartbeatResponse object
   * @throws IOException
   */
  public static HeartbeatResponse read(DataInput in) throws IOException {
    HeartbeatResponse hbr = new HeartbeatResponse();
    hbr.readFields(in);
    return hbr;
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
    if (replicationRequests == null) {
      out.writeInt(0);
    } else {
      out.writeInt(replicationRequests.length);
      for (IndexLocation i : replicationRequests) {
        i.write(out);
      }
    }
    if (updatedLeases == null) {
      out.writeInt(0);
    } else {
      out.writeInt(updatedLeases.length);
      for (Lease l : updatedLeases) {
        l.write(out);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    int n = in.readInt();
    if (n > 0) {
      replicationRequests = new IndexLocation[n];
      for (int i = 0; i < n; i++) {
        replicationRequests[i] = IndexLocation.read(in);
      }
    }
    int m = in.readInt();
    if (m > 0) {
      updatedLeases = new Lease[m];
      for (int j = 0; j < m; j++) {
        updatedLeases[j] = Lease.read(in);
      }
    }
  }

  /**
   * @return the replicationRequests
   */
  public IndexLocation[] getReplicationRequests() {
    return replicationRequests;
  }

  /**
   * @return the updatedLeases
   */
  public Lease[] getUpdatedLeases() {
    return updatedLeases;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(replicationRequests);
    result = prime * result + Arrays.hashCode(updatedLeases);
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
    final HeartbeatResponse other = (HeartbeatResponse) obj;
    if (!Arrays.equals(replicationRequests, other.replicationRequests))
      return false;
    if (!Arrays.equals(updatedLeases, other.updatedLeases))
      return false;
    return true;
  }

}
