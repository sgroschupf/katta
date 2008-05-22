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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.TestUtils;

public class ShardedIndexesTest extends TestUtils {

  private static String SIT_INDEX_ONE = getNextIndex();
  private static String SIT_INDEX_TWO = getNextIndex();
  private static String SIT_INDEX_THREE = getNextIndex();
  private static ShardedIndexes indexes = null;

  protected void setUp() throws Exception {
    super.setUp();
    if (indexes == null) {
      indexes = new ShardedIndexes();
      indexes.createShardedIndex(SIT_INDEX_ONE);
      indexes.createShardedIndex(SIT_INDEX_TWO);
    }
  }

  public void testIsSharded() {
    assertTrue(SIT_INDEX_TWO, indexes.isSharded(SIT_INDEX_ONE));
    assertFalse(SIT_INDEX_THREE, indexes.isSharded(SIT_INDEX_THREE));
  }

  public void testGet() {
    int size = indexes.getShards(SIT_INDEX_TWO).length;
    indexes.createShardedIndex(SIT_INDEX_TWO);
    indexes.createShardedIndex(SIT_INDEX_TWO);
    String[] shards = indexes.getShards(SIT_INDEX_TWO);
    assertEquals(size + 2, shards.length);
    assertEquals(SIT_INDEX_TWO + Constants.SHARD_CHAR + "0", shards[0]);
    assertEquals(SIT_INDEX_TWO + Constants.SHARD_CHAR + "1", shards[1]);
    shards = indexes.getShards(SIT_INDEX_THREE);
    assertNull(shards);
  }

  public void testGetNextShard() {
    int length = indexes.getShards(SIT_INDEX_ONE).length;
    String shard = indexes.createShardedIndex(SIT_INDEX_ONE);
    assertEquals(SIT_INDEX_ONE + Constants.SHARD_CHAR + length, shard);

  }

  public void testGetRandomShard() {
    String shard = indexes.getRandomShard(SIT_INDEX_THREE);
    assertNull(shard);
    assertNull(indexes.getShards(SIT_INDEX_THREE));
    indexes.createShardedIndex(SIT_INDEX_THREE);
    shard = indexes.createShardedIndex(SIT_INDEX_THREE);
    assertEquals(SIT_INDEX_THREE + Constants.SHARD_CHAR + 1, shard);

    int length = indexes.getShards(SIT_INDEX_ONE).length;
    indexes.createShardedIndex(SIT_INDEX_ONE);
    indexes.createShardedIndex(SIT_INDEX_ONE);
    indexes.createShardedIndex(SIT_INDEX_ONE);
    String result = indexes.getRandomShard(SIT_INDEX_ONE);
    String[] shards = indexes.getShards(SIT_INDEX_ONE);
    assertEquals(length + 3, shards.length);
    Set<String> results = new HashSet<String>();
    for (String s : shards) {
      results.add(s);
    }
    assertTrue(results.contains(result));
  }

  public void testToString() {
    assertNotNull(indexes.toString());
  }

  public void testAdd() {
    int length = indexes.getShards(SIT_INDEX_ONE).length;
    indexes.createShardedIndex(SIT_INDEX_ONE);
    String[] shards = indexes.getShards(SIT_INDEX_ONE);
    assertEquals(SIT_INDEX_ONE + Constants.SHARD_CHAR + "0", shards[0]);
    assertEquals(length + 1, shards.length);
  }

  public void testGetShards() {
    int originalSize = indexes.getShards(SIT_INDEX_ONE).length;
    indexes.createShardedIndex(SIT_INDEX_ONE);
    indexes.createShardedIndex(SIT_INDEX_ONE);
    indexes.createShardedIndex(SIT_INDEX_ONE);
    String[] shards = indexes.getShards(SIT_INDEX_ONE);
    assertEquals(originalSize + 3, shards.length);
    assertEquals(SIT_INDEX_ONE + Constants.SHARD_CHAR + "0", shards[0]);
    assertEquals(SIT_INDEX_ONE + Constants.SHARD_CHAR + "1", shards[1]);
    assertEquals(SIT_INDEX_ONE + Constants.SHARD_CHAR + "2", shards[2]);
  }

  public void testSize() {
    int originalSize = indexes.getShards(SIT_INDEX_THREE).length;
    indexes.createShardedIndex(SIT_INDEX_THREE);
    assertEquals(originalSize + 1, indexes.getShards(SIT_INDEX_THREE).length);
    indexes.createShardedIndex(SIT_INDEX_THREE);
    assertEquals(originalSize + 2, indexes.getShards(SIT_INDEX_THREE).length);
    indexes.createShardedIndex(SIT_INDEX_THREE);
    assertEquals(originalSize + 3, indexes.getShards(SIT_INDEX_THREE).length);
  }
}
