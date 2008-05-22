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
import java.util.Set;

public abstract class AbstractIndexes extends TestUtils {

  protected final static String NN_INDEX_ONE = getNextIndex();
  private static DataNodeStatus[] status = new DataNodeStatus[4];
  private static IndexLocation[][] setup = new IndexLocation[3][];
  protected final static IndexLocation[] empty = {};

  /**
   * @return the status
   */
  public static DataNodeStatus[] getStatus() {
    return status;
  }

  /**
   * @return the setup
   */
  public static IndexLocation[][] getSetup() {
    return setup;
  }

  protected void setUp() throws Exception {
    super.setUp();
    if (status[0] == null) {

      // indexes 0 and 1 are on the first machine

      status[0] = makeDataNodeStatus(MACHINES[0], RACKS[0]);
      setup[0] = new IndexLocation[2];
      setup[0][0] = new IndexLocation(status[0].getAddress(), new IndexVersion(
          NN_INDEX_ONE), IndexState.LIVE);
      IndexVersion next = setup[0][0].getIndexVersion().nextVersion();
      setup[0][1] = new IndexLocation(status[0].getAddress(), next,
          IndexState.LIVE);

      // index 2 is on the second machine

      status[1] = makeDataNodeStatus(MACHINES[1], RACKS[0]);
      setup[1] = new IndexLocation[1];
      setup[1][0] = new IndexLocation(status[1].getAddress(), next,
          IndexState.REPLICATING);

      // index 3 is on third machine

      status[2] = makeDataNodeStatus(MACHINES[2], RACKS[1]);
      setup[2] = new IndexLocation[1];
      setup[2][0] = new IndexLocation(status[2].getAddress(), next,
          IndexState.LIVE);

      status[3] = makeDataNodeStatus(MACHINES[3], RACKS[0]);
    }
  }

  protected static boolean containsAddress(Set<InetSocketAddress> addresses,
      DataNodeStatus theStatus) {
    return addresses.contains(theStatus.getAddress());
  }
}
