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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetworkTopology;

public class DataNodeStatusInformationTest extends TestUtils {

  private DataNodeStatus dnsi = null;
  private DataNodeConfiguration dnc = null;

  protected void setUp() throws Exception {
    super.setUp();
    int port = conf.getInt("dlucene.datanode.port",
        Constants.DLUCENE_DATANODE_PORT);
    InetSocketAddress addr = new InetSocketAddress(MACHINES[0], port);
    dnc = new DataNodeConfiguration(conf, addr,
        NetworkTopology.DEFAULT_RACK);
    dnsi = new DataNodeStatus(dnc, conf);
  }

  public void testNullArguments() throws Exception {
    try {
      dnsi.write(null);
      fail("write() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    try {
      dnsi.readFields(null);
      fail("readFields() should have thrown an exception!");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  public void testUpdateUsage() throws Exception {
    dnsi.updateUsage();
    assertTrue(dnsi.getCapacity() != 0);
    assertTrue(dnsi.getCapacityRemaining() != 0);
    assertTrue(dnsi.getCapacityUsed() != 0);
  }

  public void testGetAddress() {
    assertTrue(dnsi.getAddress().getHostName().contains(MACHINES[0]));
  }

  public void testGetRack() {
    assertEquals(NetworkTopology.DEFAULT_RACK, dnsi.getRack());
  }

  public void testWriteAndRead() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    dnsi.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    DataNodeStatus four = DataNodeStatus.read(in);
    assertEquals(four.getRack(), dnsi.getRack());
    assertEquals(four.getAddress(), dnsi.getAddress());
  }

}
