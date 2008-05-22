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

import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.net.NetUtils;

/**
 * Unit tests for UncommittedIndexes.
 */
public class UncommittedIndexesTest extends TestUtils {

  private static UncommittedIndexes indexes = new UncommittedIndexes();
  private static String L_INDEX_ONE = getNextIndex();
  private static InetSocketAddress addr = NetUtils
      .createSocketAddr(DATANODE_ADDRESS_ONE);
  private static IndexVersion iv = new IndexVersion(L_INDEX_ONE);
  private static IndexLocation location = new IndexLocation(addr, iv.nextVersion(),
      IndexState.LIVE);

  protected void setUp() throws Exception {
    super.setUp();
    indexes.add(location);
  }
  
  /**
   * Test UncommittedIndexes.add().
   */
  public void testAdd() {
    assertEquals(location, indexes.get(L_INDEX_ONE));
  }

  /**
   * Test UncommittedIndexes.remove().
   */
  public void testRemove() {
    assertEquals(location, indexes.get(L_INDEX_ONE));
    indexes.remove(location);
    assertNull(indexes.get(L_INDEX_ONE));
  }
}
