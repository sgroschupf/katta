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

import org.apache.hadoop.contrib.dlucene.IndexLocation;

/**
 * Data structure that keeps track of indexes with uncommitted changes.
 */
class UncommittedIndexes {
  /** a map of uncommitted indexes */
  private Map<String, IndexLocation> indexes = null;

  /**
   * Constructor.
   */
  UncommittedIndexes() {
    indexes = new HashMap<String, IndexLocation>();
  }

  /**
   * Add an uncommitted index.
   * 
   * @param location the index location
   */
  void add(IndexLocation location) {
    indexes.put(location.getIndexVersion().getName(), location);
  }

  /**
   * Remove an uncommitted index.
   * 
   * @param location the index location
   */
  void remove(IndexLocation location) {
    String name = location.getIndexVersion().getName();
    if (indexes.containsKey(name)) {
      if (indexes.get(name).equals(location)) {
        indexes.remove(name);
      }
    }
  }

  /**
   * Get the location of an uncommitted index.
   * 
   * @param indexName the index name
   * @return the index location
   */
  IndexLocation get(String indexName) {
    return indexes.containsKey(indexName) ? indexes.get(indexName) : null;
  }
}
