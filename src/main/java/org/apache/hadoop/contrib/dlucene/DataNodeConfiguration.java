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
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.contrib.dlucene.network.Network;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;

/**
 * DataNode configuration information.
 */
public class DataNodeConfiguration implements Writable {

  /** The root directory on the datanode for Lucene indexes. */
  private File rootDir = null;

  /** The address of the datanode. */
  private InetSocketAddress addr = null;

  /** The location of this datanode in the network. */
  private String rack = NetworkTopology.DEFAULT_RACK;

  /**
   * The constructor.
   * 
   * @param machineName the machine name
   * @param port the port
   * @param rack the rack
   * @param shardFolder the folder containing shards
   * @throws IOException
   */
  public DataNodeConfiguration(InetSocketAddress addr, String rack, String shardFolder) throws IOException {
    this.addr = addr;
    this.rack = rack;
    File mainRoot = new File(shardFolder);
    Utils.checkDirectoryIsReadableWritable(mainRoot);
    rootDir = new File(mainRoot, Network.convertInetSocketAddress(addr));
  }

  /**
   * Deserialize this object.
   * 
   * @param in the input stream
   * @return the configuration the Hadoop configuration
   * @throws IOException
   */
  public static DataNodeConfiguration read(DataInput in) throws IOException {
    InetSocketAddress addr = new InetSocketAddress(
        Constants.DATANODE_DEFAULT_NAME_VALUE, Constants.DLUCENE_DATANODE_PORT);
    DataNodeConfiguration dnc = new DataNodeConfiguration(addr, Constants.DATANODE_RACK_NAME, Constants.DEFAULT_ROOT_DIR);
    dnc.readFields(in);
    return dnc;
  }

  /**
   * @return the rootDir
   */
  public File getRootDir() {
    return rootDir;
  }

  /**
   * @return the addr
   */
  public InetSocketAddress getAddress() {
    return addr;
  }

  /**
   * @return the rack location
   */
  public String getRack() {
    return rack;
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
    Text.writeString(out, rootDir.getPath());
    Text.writeString(out, Network.convertInetSocketAddress(addr));
    Text.writeString(out, rack);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    this.rootDir = new File(Text.readString(in));
    this.addr = NetUtils.createSocketAddr(Text.readString(in));
    this.rack = Text.readString(in);
  }

}
