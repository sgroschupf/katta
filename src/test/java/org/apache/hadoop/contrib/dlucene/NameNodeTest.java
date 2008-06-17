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
import java.util.Set;

import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

public class NameNodeTest extends AbstractIndexes {

  private static NameNode n = null;

  private static Lease[] leases = null;

  protected void setUp() throws Exception {
    super.setUp();
    if (n == null) {
      final ZKClient zkclient = new ZKClient(new ZkConfiguration());
      n = new NameNode(zkclient, conf, new InetSocketAddress(Constants.HOST, TestUtils
          .getNextPort()));
    }
  }

  public void testGetProtocolVersion() throws Exception {
    assertTrue(n
        .getProtocolVersion(ClientToNameNodeProtocol.class.getName(), 0) == ClientToNameNodeProtocol.VERSION_ID);
  }

  public void testGetProtocolVersionNull() {
    try {
      n.getProtocolVersion(DataNodeToDataNodeProtocol.class.getName(), 0);
      fail("Should throw an exception");
    } catch (IOException io) {
      //
    }
  }

  public void testGetSearchableIndexes() throws Exception {
    Set<IndexLocation> indexes = toSet(n.getSearchableIndexes());
    assertEquals(3, indexes.size());
    assertTrue(indexes.contains(getSetup()[0][0]));
    assertTrue(indexes.contains(getSetup()[0][1]));
    assertTrue(indexes.contains(getSetup()[2][0]));
  }

  public void testHeartbeat() throws Exception {
    for (int i = 0; i <= 2; i++) {
    }
    n.doHeartbeat();
  }
  
  public void testToString() {
    assertNotNull(n.toString());
  }
}
