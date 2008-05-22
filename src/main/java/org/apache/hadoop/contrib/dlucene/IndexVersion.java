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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Data structure describing a particular version of an Index.
 */
public class IndexVersion implements Comparable<IndexVersion>, Writable {

  /** Unique name of the index. */
  private String name;

  /** The version of the index. */
  private int version;

  /**
   * Constructor.
   * 
   * @param name the name of the index
   * @param version the version of the index
   */
  public IndexVersion(String name, int version) {
    Utils.checkArgs(name);
    this.name = name;
    this.version = version;
  }

  /**
   * Constructor.
   * 
   * @param name the name of the index
   */
  public IndexVersion(String name) {
    Utils.checkArgs(name);
    this.name = name;
    this.version = 0;
  }

  /**
   * Constructor.
   */
  protected IndexVersion() {
    //
  }

  /**
   * Deserialize this object.
   * 
   * @param in the input stream
   * @return the indexVersion
   * @throws IOException
   */
  public static IndexVersion read(DataInput in) throws IOException {
    IndexVersion dnc = new IndexVersion();
    dnc.readFields(in);
    return dnc;
  }

  /**
   * @return the next version of this index
   */
  public IndexVersion nextVersion() {
    return new IndexVersion(name, 1 + version);
  }

  /**
   * @return the id
   */
  public String getName() {
    return name;
  }

  /**
   * @return the version
   */
  public int getVersion() {
    return version;
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
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + version;
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
    if (!(obj instanceof IndexVersion))
      return false;
    final IndexVersion other = (IndexVersion) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (version != other.version)
      return false;
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    return "IndexVersion id: " + name + " version: " + version;
  }

  // ///////////////////////////////////
  // Comparable
  // ///////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(IndexVersion d) throws ClassCastException {
    Utils.checkArgs(name);
    int ids = name.compareTo(d.getName());
    return ids != 0 ? ids : d.getVersion() - version;
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
    Text.writeString(out, name);
    out.writeInt(version);

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    this.name = Text.readString(in);
    this.version = in.readInt();
  }
}
