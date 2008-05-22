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
package org.apache.hadoop.contrib.dlucene.data;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.Lease;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.net.NetUtils;

public class NameNodeLeaseManagerTest extends TestUtils {

  private static String IVT_INDEX_ONE = getNextIndex();
  private static IndexVersion indexVersionOne = new IndexVersion(IVT_INDEX_ONE);
  private static InetSocketAddress addr = NetUtils.createSocketAddr(DATANODE_ADDRESS_ONE);
  private static long leaseLength = 1000;
  private static Lease lease = null ;
  private static Lease lease2 = null ;
  
  private static NameNodeLeaseManager nnlm = null;
  
  protected void setUp() throws Exception {
    super.setUp();
    if (nnlm == null) {
      nnlm = new NameNodeLeaseManager(leaseLength);
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testGetLease() {
    lease = null;
    try {
      lease = nnlm.getLease(indexVersionOne, addr);
    } catch (IOException le) {
      fail("Should not throw an exception here");
    }
    assertTrue(lease != null);
    assertEquals(indexVersionOne, lease.getIndex());
    assertEquals(addr, lease.getAddress());
    InetSocketAddress addr2 = NetUtils.createSocketAddr(DATANODE_ADDRESS_TWO);
    try {
      nnlm.getLease(indexVersionOne, addr2);
      fail("Should throw an exception here");
    } catch (IOException le) {
      // okay - should throw an exception here
    }
  }

  public void testCheckLeases() {
    nnlm.checkLeases();
  }

  public void testUpdateLeases() {
    String IVT_INDEX_TWO = getNextIndex();
    IndexVersion indexVersionTwo = new IndexVersion(IVT_INDEX_TWO);
    Lease leaseThree = null;
    try {
      leaseThree = nnlm.getLease(indexVersionTwo, addr);
    } catch (IOException le) {
      fail("Should not throw an exception here");
    }
    Lease[] leases = {lease, leaseThree};
    leases = nnlm.updateLeases(leases);
    assertEquals(lease, leases[0]);
    assertEquals(leaseThree, leases[1]);
  }
  
  public void testRelinquishLease() {
    try {
      nnlm.relinquishLease(lease);
    } catch (IOException le) {
      fail("Should not throw an exception here");
    }
    try {
      nnlm.relinquishLease(lease2);
      fail("Should throw an exception here");
    } catch (IOException le) {
      // okay - should throw an exception here
    }
  }



}
