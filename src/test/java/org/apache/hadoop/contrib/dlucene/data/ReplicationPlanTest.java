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

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;

public class ReplicationPlanTest extends TestUtils {

  private ReplicationPlan rp = null;
  private static String INDEX_ONE = getNextIndex();

  protected void setUp() throws Exception {
    super.setUp();
    rp = new ReplicationPlan();
  }

  public void testAdd() {
    IndexVersion iv = new IndexVersion(INDEX_ONE);
    InetSocketAddress addr1 = new InetSocketAddress(MACHINES[0], getNextPort());
    InetSocketAddress addr2 = new InetSocketAddress(MACHINES[1], getNextPort());
    rp.add(iv, addr1, addr2);
    assertEquals(1, rp.numberOfTasks(addr1));
    assertEquals(0, rp.numberOfTasks(addr2));
    Map<InetSocketAddress, IndexLocation[]> results = rp.get();
    assertEquals(1, rp.numberOfTasks(addr1));
    assertNull(results.get(addr2));
  }
}
