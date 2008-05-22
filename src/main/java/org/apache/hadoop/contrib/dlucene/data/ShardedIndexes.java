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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.Constants;

/**
 * Client-side data structure which keeps track of sharded indexes.
 */
public class ShardedIndexes {
  
  /** Logging. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.ShardedIndexes");

  /** the sharded indexes. */
  private Map<String, ShardedIndex> indexes = null;

  /** For generating random numbers. */
  private static final Random RANDOM = new Random();

  /**
   * Constructor.
   */
  public ShardedIndexes() {
    indexes = new HashMap<String, ShardedIndex>();
  }

  /**
   * Is the index sharded?
   * 
   * @param indexName the index name
   * @return is the index sharded?
   */
  public boolean isSharded(String indexName) {
    return indexes.containsKey(indexName);
  }

  /**
   * Get the names of the shards in this index.
   * 
   * @param indexName the index name
   * @return the shards in this index
   */
  public String[] getShards(String indexName) {
    if (isSharded(indexName)) {
      if (indexes.containsKey(indexName)) {
        return indexes.get(indexName).getShards();
      }
    }
    return null;
  }

  /**
   * Get a shard at random.
   * 
   * @param indexName the index name
   * @return the shard name
   */
  public String getRandomShard(String indexName) {
    return indexes.containsKey(indexName) ? indexes.get(indexName)
        .getRandomShard() : null;
  }

  /**
   * Create a sharded index.
   * 
   * @param indexName the index name
   * @return the name of the first shard
   */
  public String createShardedIndex(String indexName) {
    ShardedIndex si = null;
    if (indexes.containsKey(indexName)) {
      LOG.info("Index " + indexName + " already exists");
      si = indexes.get(indexName);
    } else {
      si = new ShardedIndex(indexName);
    }
    String shard = si.addNextShard();
    indexes.put(indexName, si);
    return shard;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    if (indexes.keySet() != null) {
      for (String key : indexes.keySet()) {
        result.append("[" + indexes.get(key) + "]");
      }
    }
    return result.toString();
  }
  
  /**
   * A sharded index.
   */
  private final class ShardedIndex {

    /** the name of the index. */
    private String name = null;

    /** the shards of the index. */
    private SortedMap<Integer, Shard> shards = null;
    
    /**
     * Constructor.
     *
     * @param indexName the index name
     */
    private ShardedIndex(String indexName) {
      this.name = indexName;
      shards = new TreeMap<Integer, Shard>();
    }

    /**
     * Get the names of the shards in this index.
     *
     * @return the shards
     */
    private String[] getShards() {
      String[] result = new String[shards.size()];
      for (int key : shards.keySet()) {
        result[key] = shards.get(key).get();
      }
      return result;
    }

    /**
     * Get the next shard.
     *
     * @return the next shard.
     */
    private String addNextShard() {
      Shard shard = new Shard(name, shards.size());
      LOG.info("Adding shard " + shard.number + " name " + name);
      shards.put(shard.number, shard);
      return shard.get();
    }
        
    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append(name + ", ");
      for (int i : shards.keySet()) {
        result.append("[" + i + ", " + shards.get(i) + "]");
      }
      return result.toString();
    }

    /**
     * Get a shard at random.
     *
     * @return A random shard name
     */
    private String getRandomShard() {
      return shards.size() > 0 ? shards.get(RANDOM.nextInt(shards.size()))
          .get() : null;
    }

    /**
     * An individual shard.
     */
    private final class Shard {

      /** the name of the shard. */
      private String shardName = null;

      /** the shard number. */
      private int number;

      /**
       * Constructor.
       *
       * @param name the name of a Shard
       * @param number the Shard number
       */
      private Shard(String name, int number) {
        this.shardName = name;
        this.number = number;
      }

      /**
       * Get the index name of a shard.
       *
       * @return the index name of the shard
       */
      private String get() {
        return shardName + Constants.SHARD_CHAR + number;
      }

      /*
       * (non-Javadoc)
       *
       * @see java.lang.Object#toString()
       */
      public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("[" + get() + " ]");
        return result.toString();
      }
    }
  }
}
