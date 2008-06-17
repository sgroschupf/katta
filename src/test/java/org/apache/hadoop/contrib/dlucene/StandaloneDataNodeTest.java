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

import java.net.InetSocketAddress;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.net.NetUtils;

/**
 * Unit tests for SimpleClient.java.
 */
public class StandaloneDataNodeTest extends TestUtils {

  private static SimpleClient client = new SimpleClient(conf);

  /**
   * Test getting connections to datanodes.
   * 
   * @throws Exception
   */
  public void testDataNodeTwo() throws Exception {
    final ZKClient zkclient = new ZKClient(new ZkConfiguration());
    DataNode d1 = new DataNode(zkclient, conf, new InetSocketAddress(Constants.HOST,
        getNextPort()), null, USE_RAM_INDEX_FOR_TESTS);
    DataNode d2 = new DataNode(zkclient, conf, new InetSocketAddress(Constants.HOST,
        getNextPort()), null, USE_RAM_INDEX_FOR_TESTS);
    assertTrue(client.getDataNode(d1.nodeAddr) != null);
    assertTrue(client.getDataNode(d2.nodeAddr) != null);
  }

  /**
   * Test getting connections to namenodes.
   * 
   * @throws Exception
   */
  public void testDataNodeThree() throws Exception {
    final ZKClient zkclient = new ZKClient(new ZkConfiguration());
    NameNode n1 = NameNode.createNode(zkclient, conf, new InetSocketAddress(
        Constants.HOST, getNextPort()));
    assertTrue(client.getNameNode(n1.nodeAddr) != null);
    System.out.println(n1.nodeAddr);
    DataNode d1 = DataNode.createNode(zkclient, conf, new InetSocketAddress(
        Constants.HOST, getNextPort()), n1.nodeAddr, USE_RAM_INDEX_FOR_TESTS);
    DataNode d2 = DataNode.createNode(zkclient, conf, new InetSocketAddress(
        Constants.HOST, getNextPort()), n1.nodeAddr, USE_RAM_INDEX_FOR_TESTS);
    assertTrue(client.getDataNode(d1.nodeAddr) != null);
    assertTrue(client.getDataNode(d2.nodeAddr) != null);
    ClientToNameNodeProtocol p3 = client.getNameNode(n1.nodeAddr);
    assertTrue(p3 != null);
    String s = p3.getDataNode();
    System.out.println(s);
    InetSocketAddress addr = NetUtils.createSocketAddr(s);
    ClientToDataNodeProtocol p4 = client.getDataNode(addr);
    assertTrue(p4 != null);
  }

}
