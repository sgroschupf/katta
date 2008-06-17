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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNodeConfiguration;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;
import org.apache.hadoop.net.NetworkTopology;

public class DataNodeIndexesTest extends TestUtils {

  private static String DNI_INDEX_ONE = getNextIndex();
  private static IndexVersion index = new IndexVersion(DNI_INDEX_ONE);
  private DataNodeIndexes indexes = null;
  private DataNodeConfiguration dnc = null;

  protected void setUp() throws Exception {
    super.setUp();
    int port = conf.getInt("dlucene.datanode.port",
        Constants.DLUCENE_DATANODE_PORT);
    InetSocketAddress addr = new InetSocketAddress(MACHINES[0], port);
    dnc = new DataNodeConfiguration(addr, NetworkTopology.DEFAULT_RACK, Constants.DEFAULT_ROOT_DIR);
    indexes = new DataNodeIndexes(dnc);
  }

  public void testAddIndex() throws Exception {
    IndexVersion iv = new IndexVersion(DNI_INDEX_ONE);
    IndexLocation location = new IndexLocation(dnc.getAddress(), iv,
        IndexState.LIVE);
    indexes.add(location);
    IndexLocation[] result = indexes.getIndexes();
    assertEquals(1, result.length);
    assertEquals(location, result[0]);
    IndexVersion l = indexes.getPrimaryIndex(DNI_INDEX_ONE).getIndexVersion();
    assertEquals(location.getIndexVersion(), l);
  }

  public void testAddIndexTwo() throws Exception {
    indexes.add(new IndexLocation(dnc.getAddress(), index, IndexState.LIVE));
    assertEquals(1, indexes.getIndexes().length);
    assertEquals(index, indexes.getIndexes()[0].getIndexVersion());
    // check if we try to create the index again it throws an exception
    try {
      indexes.add(new IndexLocation(dnc.getAddress(), index, IndexState.LIVE));
      fail("addIndex() should have thrown an exception!");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testCommitIndex() throws Exception {
    String index1 = getNextIndex();
    IndexVersion iv = new IndexVersion(index1);
    IndexLocation location = new IndexLocation(dnc.getAddress(), iv,
        IndexState.UNCOMMITTED);
    indexes.add(location);
    indexes.setIndexState(location, IndexState.UNCOMMITTED, IndexState.LIVE);
    IndexLocation[] result = indexes.getIndexes();
    assertEquals(1, result.length);
    assertEquals(location, result[0]);
    IndexVersion l = indexes.getPrimaryIndex(index1).getIndexVersion();
    assertEquals(iv, l);
    IndexLocation location2 = new IndexLocation(dnc.getAddress(), iv
        .nextVersion(), IndexState.LIVE);
    indexes.add(location2);
    try {
      indexes.setIndexState(location2, IndexState.UNCOMMITTED, IndexState.LIVE);
      fail("Should have thrown an exception - no uncommitted changes");
    } catch (Exception e) {
      //
    }
  }

  public void testCommitIndexTwo() throws Exception {
    IndexLocation temp = new IndexLocation(dnc.getAddress(), index,
        IndexState.LIVE);
    // try committing an index that does not exist
    try {
      indexes.setIndexState(temp, IndexState.UNCOMMITTED, IndexState.LIVE);
      fail("commitIndex() should have thrown an exception!");
    } catch (Exception expected) {
      // expected
    }
    temp = new IndexLocation(dnc.getAddress(), index, IndexState.UNCOMMITTED);
    indexes.add(temp);
    assertEquals(IndexState.UNCOMMITTED, indexes.getIndexes()[0].getState());
    indexes.setIndexState(temp, IndexState.UNCOMMITTED, IndexState.LIVE);
    assertEquals(IndexState.LIVE, indexes.getIndexes()[0].getState());
  }

  public void testGetPrimaryIndex() throws Exception {
    indexes.add(new IndexLocation(dnc.getAddress(), index, IndexState.LIVE));
    assertEquals(index, indexes.getPrimaryIndex(DNI_INDEX_ONE).getIndexVersion());
    IndexVersion index2 = index.nextVersion();
    indexes.add(new IndexLocation(dnc.getAddress(), index2,
        IndexState.UNCOMMITTED));
    assertEquals(index2, indexes.getPrimaryIndex(DNI_INDEX_ONE).getIndexVersion());
  }

  public void testGetArrayOfIndexes() throws Exception {
    IndexLocation l1 = new IndexLocation(dnc.getAddress(), index,
        IndexState.LIVE);
    indexes.add(l1);
    IndexVersion index2 = index.nextVersion();
    IndexLocation l2 = new IndexLocation(dnc.getAddress(), index2,
        IndexState.UNCOMMITTED);
    indexes.add(l2);
    IndexLocation[] locations = indexes.getIndexes();
    assertEquals(2, locations.length);
    Set<IndexVersion> locationSet = new HashSet<IndexVersion>();
    for (IndexLocation location : locations) {
      locationSet.add(location.getIndexVersion());
    }
    assertTrue(locationSet.contains(index));
    assertTrue(locationSet.contains(index2));
    int pos = locations[0].equals(l2) ? 0 : 1;
    assertEquals(IndexState.UNCOMMITTED, locations[pos].getState());
    indexes.setIndexState(locations[pos], IndexState.UNCOMMITTED,
        IndexState.LIVE);
    assertEquals(IndexState.LIVE, locations[pos].getState());
  }

  public void testGetNewIndexDirectory() throws Exception {
    String id = "someStrangeNewId";
    int version = 6055;
    IndexVersion newIndex = new IndexVersion(id, version);
    File file = indexes.getIndexDirectory(newIndex);
    assertTrue(file.getAbsolutePath().contains(id));
    assertTrue(file.getName().contains(Integer.valueOf(version).toString()));
  }

  public void testGetIndexDirectoryNull() throws Exception {
    // test for an index that does not exist
    try {
      indexes.getKnownIndexDirectory(index);
      fail("getIndexDirectory() should have thrown an exception");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testGetIndexDirectory() throws Exception {
    indexes.add(new IndexLocation(dnc.getAddress(), index, IndexState.LIVE));
    File file = indexes.getKnownIndexDirectory(index);
    assert (file != null);
  }

  public void testHasIndex() throws Exception {
    // test for an index that does not exist
    assertFalse(indexes.hasIndex(DNI_INDEX_ONE));
    indexes.add(new IndexLocation(dnc.getAddress(), index, IndexState.LIVE));
    assertTrue(indexes.hasIndex(DNI_INDEX_ONE));
  }

  public void testToString() throws Exception {
    assertNotNull(indexes.toString());
  }
}
