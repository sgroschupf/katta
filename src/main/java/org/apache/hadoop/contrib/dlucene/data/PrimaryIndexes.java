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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexVersion;

/**
 * Data structure storing the latest version of each index.
 */
public class PrimaryIndexes {

  /** Logging. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.PrimaryIndexes");
  
  /** a map of indexes on to locations. */
  private Map<String, Set<IndexVersion>> indexes =  new TreeMap<String, Set<IndexVersion>>();

  /**
   * Add an index.
   * 
   * @param location the index
   */
  public void add(IndexVersion version) {
    LOG.debug("Adding " + version + " as a primary index");
    String id = version.getName();
    Set<IndexVersion> versionSet = indexes.containsKey(id) ? indexes.get(id)
	: new TreeSet<IndexVersion>();
    versionSet.add(version);
    indexes.put(id, versionSet);
  }

  /**
   * Remove an index.
   * 
   * @param location the index
   */
  public void remove(IndexVersion version) {
    String id = version.getName();
    if (!indexes.containsKey(id)) {
      throw new IllegalArgumentException(
	  "No such IndexLocation in the list of primary indexes: " + id);
    }
    Set<IndexVersion> locationSet = indexes.get(id);
    locationSet.remove(version);
    if (locationSet.size() > 0) {
      indexes.put(id, locationSet);
    } else {
      indexes.remove(id);
    }
  }

  /**
   * Get the location of the primary index.
   * 
   * @param indexName the index
   * @return the index location
   */
  public IndexVersion get(String indexName) {
    if (indexes.containsKey(indexName)) {
      if (indexes.get(indexName).size() > 0) {
	return indexes.get(indexName).iterator().next();
      }
    }
    StringBuffer s = new StringBuffer();
    s.append("There is no primary index for " + indexName + " the index names are: \n");
    if (indexes.keySet().size() == 0) {
      s.append("There are no indexes\n");
    }
    for (String r : indexes.keySet()) {
      s.append(r + "\n");
    }
     throw new RuntimeException(s.toString());
  }

  /**
   * Does this index exist?
   * 
   * @param indexName the index
   * @return does this index exist?
   */
  public boolean contains(String indexName) {
    return indexes.containsKey(indexName);
  }
}
