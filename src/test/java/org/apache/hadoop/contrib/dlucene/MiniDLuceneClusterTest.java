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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

/**
 * Test the MiniCluster test assembly
 */
public class MiniDLuceneClusterTest extends TestCase {

  private static MiniDLuceneCluster cluster = null;
  private static Configuration conf = new Configuration();
  private static SimpleClient client = new SimpleClient(conf);
  private static boolean lastTest = false;
  private InetSocketAddress nameNodeAddr = null;

  protected void setUp() throws Exception {
    super.setUp();
    if (cluster == null) {
      cluster = new MiniDLuceneCluster(conf, 2);
    }
    nameNodeAddr = new InetSocketAddress(Constants.HOST, cluster
        .getNameNodePort());
  }

  protected void tearDown() throws Exception {
    if (lastTest) {
      cluster.shutdown();
    }
  }

  /**
   * Test the name node is working.
   * 
   * @throws Exception
   */
  public void testNameNode() throws Exception {
    ClientToNameNodeProtocol nameNode = client.getNameNode(nameNodeAddr);
    assertTrue(nameNode != null);
    IndexLocation[] indexes = nameNode.getSearchableIndexes();
    assertEquals(0, indexes.length);
  }

  /**
   * Test the data node is working.
   * 
   * @throws Exception
   */
  public void testDataNode() throws Exception {
    lastTest = true;
    ClientToNameNodeProtocol nameNode = client.getNameNode(nameNodeAddr);
    assertTrue(nameNode != null);
    InetSocketAddress dn = NetUtils.createSocketAddr(nameNode.getDataNode());
    ClientToDataNodeProtocol dataNode = client.getDataNode(dn);
    assertTrue(dataNode != null);
  }

}
