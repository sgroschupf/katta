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
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class SimpleClient {

  /** Hadoop configuration */
  Configuration conf = null;

  public SimpleClient(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get a connection to the NameNode
   * 
   * @return the connection to the NameNode
   * @throws IOException thrown if it cannot contact NameNode
   */
  public ClientToNameNodeProtocol getNameNode(InetSocketAddress addr)
      throws IOException {
    Utils.checkArgs(addr);
    return (ClientToNameNodeProtocol) RPC.waitForProxy(
        ClientToNameNodeProtocol.class, ClientToNameNodeProtocol.VERSION_ID,
        addr, conf);
  }

  /**
   * Get a connection to a DataNode with this socket address
   * 
   * @param addr
   * @return the connection to the DataNode
   * @throws IOException thrown if it cannot contact DataNode
   */
  public ClientToDataNodeProtocol getDataNode(InetSocketAddress addr)
      throws IOException {
    Utils.checkArgs(addr);
    return (ClientToDataNodeProtocol) RPC.waitForProxy(
        ClientToDataNodeProtocol.class, ClientToDataNodeProtocol.VERSION_ID,
        addr, conf);
  }
}
