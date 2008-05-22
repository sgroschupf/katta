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

import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;

public class IndexLocation implements Comparable<IndexLocation>, Writable {

  private IndexVersion indexVersion;

  private InetSocketAddress location;

  /**
   * The index state.
   */
  private IndexState state;

  public IndexLocation(InetSocketAddress location, IndexVersion indexVersion,
      IndexState state) {
    Utils.checkArgs(indexVersion, location, state);
    this.indexVersion = indexVersion;
    this.location = location;
    this.state = state;
  }

  protected IndexLocation() {
    //
  }

  public static IndexLocation read(DataInput in) throws IOException {
    IndexLocation dnc = new IndexLocation();
    dnc.readFields(in);
    return dnc;
  }

  /**
   * Get the IndexVersion.
   * 
   * @return the indexVersion
   */
  public IndexVersion getIndexVersion() {
    return indexVersion;
  }

  /**
   * @return the location
   */
  public InetSocketAddress getAddress() {
    return location;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((indexVersion == null) ? 0 : indexVersion.hashCode());
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IndexLocation))
      return false;
    final IndexLocation other = (IndexLocation) obj;
    if (indexVersion == null) {
      if (other.indexVersion != null)
        return false;
    } else if (!indexVersion.equals(other.indexVersion))
      return false;
    if (location == null) {
      if (other.location != null)
        return false;
    } else if (!location.equals(other.location))
      return false;
    return true;
  }

  /**
   * @return the state
   */
  public IndexState getState() {
    return state;
  }

  /**
   * @param state the state to set
   */
  public void setState(IndexState state) {
    Utils.checkArgs(state);
    this.state = state;
  }

  public String toString() {
    return "IndexLocation location: " + location + " ("
        + indexVersion.toString() + ") state : " + state;
  }

  public IndexLocation nextVersion() {
    return new IndexLocation(getAddress(), getIndexVersion().nextVersion(),
        IndexState.UNCOMMITTED);
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
    indexVersion.write(out);
    Text.writeString(out, Network.convertInetSocketAddress(getAddress()));
    out.writeInt(state.ordinal());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    indexVersion = IndexVersion.read(in);
    this.location = NetUtils.createSocketAddr(Text.readString(in));
    this.state = IndexState.values()[in.readInt()];
  }

  // ///////////////////////////////////
  // Comparable
  // ///////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(IndexLocation d) throws ClassCastException {
    return getIndexVersion().compareTo(d.getIndexVersion());
  }
}
