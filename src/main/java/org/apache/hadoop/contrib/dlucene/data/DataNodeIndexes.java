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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.contrib.dlucene.Constants;
import org.apache.hadoop.contrib.dlucene.DataNode;
import org.apache.hadoop.contrib.dlucene.DataNodeConfiguration;
import org.apache.hadoop.contrib.dlucene.IndexLocation;
import org.apache.hadoop.contrib.dlucene.IndexState;
import org.apache.hadoop.contrib.dlucene.IndexVersion;

/**
 * A data structure of indexes managed by a {@link DataNode}.
 */
class DataNodeIndexes {

  /** A map of indexes onto directories. */
  private Map<IndexVersion, File> indexDirectories = new HashMap<IndexVersion, File>();
  
  private Map<IndexVersion, IndexLocation> indexLocations = new HashMap<IndexVersion, IndexLocation>();
  
  /** The root dir for the index store. */
  private File rootDir = null;

  /** The primary indexes. */
  private PrimaryIndexes primaryIndexes = null;

  /**
   * Constructor.
   * 
   * @param dataNodeConfiguration the configuration of the DataNode
   * @throws IOException
   */
  DataNodeIndexes(DataNodeConfiguration dataNodeConfiguration) {
    this.rootDir = dataNodeConfiguration.getRootDir();
    primaryIndexes = new PrimaryIndexes();
  }

  /**
   * Add an index location.
   * 
   * @param location The IndexLocation
   * @throws IOException
   */
  void add(IndexLocation location) throws IOException {
    IndexVersion index = location.getIndexVersion();
    if (indexDirectories.containsKey(index)) {
      // index already exists
      throw new IOException("Index " + location + " already exists");
    }
    File directory = getIndexDirectory(index);
    indexDirectories.put(index, directory);
    primaryIndexes.add(index);
    indexLocations.put(index, location);
  }
  
  /**
   * Get the most up to date version of this index.
   * 
   * @param indexName the index name
   * @return the primary version of this index
   */    
  IndexLocation getPrimaryIndex(String indexName) {
    return indexLocations.get(primaryIndexes.get(indexName));
  }

  /**
   * Get all the indexes managed by this {@link DataNode}.
   * 
   * @return get the index locations
   */
  IndexLocation[] getIndexes() {
    return indexLocations.values().toArray(new IndexLocation[indexDirectories.size()]);
  }

  /**
   * Get the location of an index on the Filesystem.
   * 
   * @param index the index
   * @return a File for the index
   */
  File getIndexDirectory(IndexVersion index) {
    return new File(rootDir, index.getName() + File.separatorChar
        + Constants.VERSION_PREFIX + index.getVersion());
  }

  /**
   * Get the file corresponding to this index.
   * 
   * @param indexVersion the index
   * @return the file corresponding to the index
   * @throws IOException thrown if the index does not exist
   */
  File getKnownIndexDirectory(IndexVersion indexVersion) throws IOException {
    if (indexDirectories.containsKey(indexVersion)) {
      return indexDirectories.get(indexVersion);
    }
    throw new IOException("Datanode does not have index " + indexVersion
        + " only has " + indexDirectories.keySet());
  }

  /**
   * Change the state of an index.
   * 
   * @param location the index
   * @param oldState the current state of the index
   * @param newState the new state of the index
   * @throws IOException thrown if the index does is not in the correct state
   */
  void setIndexState(IndexLocation location, IndexState oldState,
      IndexState newState) throws IOException {
    if (location.getState() != oldState) {
      throw new IOException("Expecting " + location + " to be in " + oldState
          + " but it was " + location.getState());
    }
    location.setState(newState);
    indexLocations.put(location.getIndexVersion(), location);
    primaryIndexes.add(location.getIndexVersion());
  }

  /**
   * Check that the DataNode has this index.
   * 
   * @param indexName the index
   * @return does the DataNode have this index?
   */
  boolean hasIndex(String indexName) {
    return primaryIndexes.contains(indexName);
  }

  /**
   * Check that the DataNode has this index.
   * 
   * @param indexVersion the index
   * @return does the DataNode have this index?
   */
  boolean hasIndex(IndexVersion indexVersion) {
    return indexDirectories.containsKey(indexVersion);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append("indexLocations:\n");
    for (IndexLocation l : indexLocations.values()) {
      result.append(l.toString() + "\n");
    }
    result.append(primaryIndexes.toString() + "\n");
    return result.toString();
  }
}
