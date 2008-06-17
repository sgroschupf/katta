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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

public interface ICachedClient {

  /**
   * Create an index or add a new shard to an index.
   * 
   * @param index The index name.
   * @param sharded Is the index sharded.
   * @throws IOException
   * @throws KattaException 
   */
  public void createIndex(String index, boolean sharded)
      throws IOException, KattaException;

  /**
   * Get an IndexWriter to write to indexes.
   * 
   * @param index The index name.
   * @return An IndexUpdated object.
   */
  public IIndexUpdater getIndexUpdater(String index);

  /**
   * Get the size of an index.
   * 
   * @param index The index name.
   * @return The size of the index.
   * @throws IOException
   */
  public int size(String index) throws IOException;

  /**
   * Search an index.
   * 
   * @param index The index.
   * @param query The query.
   * @param sort The order of results.
   * @param n Number of results.
   * @return The results of the query.
   * @throws IOException
   */
  public SearchResults search(String index, Query query, Sort sort,
      int n) throws IOException;

  /**
   * @return All the index names.
   */
  public String[] getIndexes();

}
