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

import java.io.IOException;

import net.sf.katta.util.KattaException;

import org.apache.hadoop.contrib.dlucene.writable.SearchResults;
import org.apache.hadoop.contrib.dlucene.writable.WDocument;
import org.apache.hadoop.contrib.dlucene.writable.WQuery;
import org.apache.hadoop.contrib.dlucene.writable.WSort;
import org.apache.hadoop.contrib.dlucene.writable.WTerm;
import org.apache.hadoop.ipc.VersionedProtocol;

/** Interface between client and datanode. */
public interface ClientToDataNodeProtocol extends VersionedProtocol {

  /** The version of the protocol. */
  long VERSION_ID = 1L;

  /**
   * Add a document to a particular index.
   * 
   * @param index The index.
   * @param doc The document.
   * @throws IOException
   * @throws KattaException 
   */
  void addDocument(String index, WDocument doc) throws IOException, KattaException;

  /**
   * Remove documents that match a specific term from an index. 
   * 
   * @param index The index.
   * @param term The term.
   * @return The number of documents removed.
   * @throws IOException
   */
  int removeDocuments(String index, WTerm term) throws IOException, KattaException;

  /**
   * Commit a specific index.
   * 
   * @param index The index.
   * @return The IndexVersion of the committed index.
   * @throws IOException
   * @throws KattaException 
   */
  IndexVersion commitVersion(String index) throws IOException, KattaException;

  /**
   * Create a new index.
   * 
   * @param index The index. 
   * @return the IndexVersion of the new index.
   * @throws IOException
   * @throws KattaException 
   */
  IndexVersion createIndex(String index) throws IOException, KattaException;

  /**
   * Add the contents of an index to another index.
   * 
   * @param index The index. 
   * @param indexToAdd The location of the index to add.
   * @throws IOException
   * @throws KattaException 
   */
  void addIndex(String index, IndexLocation indexToAdd) 
    throws IOException, KattaException;

  /**
   * Search a specific index returning the top n hits ordered by
   * sort.
   * 
   * @param i The index to search.
   * @param query The query.
   * @param sort The sort to apply to results.
   * @param n The maximum number of hits to return.
   * @return The results.
   * @throws IOException
   */
  SearchResults search(IndexVersion i, WQuery query, WSort sort, int n)
      throws IOException;

  /**
   * The number of documents in an index.
   * 
   * @param index The index.
   * @return the siz
   * @throws IOException
   */
  int size(String index) throws IOException;
}
