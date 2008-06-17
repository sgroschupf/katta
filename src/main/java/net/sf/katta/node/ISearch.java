/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package net.sf.katta.node;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.lucene.queryParser.ParseException;

public interface ISearch extends VersionedProtocol {

  /**
   * Returns all Hits that match the query. This might be significant slower as
   * {@link #search(IQuery, DocumentFrequenceWritable, String[], int)} since we
   * replace count with {@link Integer.MAX_VALUE}.
   * 
   * @param query
   * @param freqs
   * @param shardNames
   *            A array of shard names to search in.
   * @return
   * @throws ParseException
   * @throws IOException
   */
  public HitsMapWritable search(IQuery query, DocumentFrequenceWritable freqs, String[] shardNames) throws IOException;

  /**
   * @param query
   * @param freqs
   * @param shardNames
   * @param count
   *            the top n high score hits
   * @return
   * @throws ParseException
   * @throws IOException
   */
  public HitsMapWritable search(IQuery query, DocumentFrequenceWritable freqs, String[] shardNames, int count)
      throws IOException;

  /**
   * Returns the number of documents a term occurs in. In a distributed search
   * environment, we need to get this first and then query all nodes again with
   * this information to ensure we compute TF IDF correctly. See
   * {@link http://lucene.apache.org/java/2_3_0/api/org/apache/lucene/search/Similarity.html}
   * 
   * @param input
   * @param shards
   * @return
   * @throws IOException
   * @throws ParseException
   */
  public DocumentFrequenceWritable getDocFreqs(IQuery input, String[] shards) throws IOException;

  /**
   * Returns only the request fields of a lucene document.
   * 
   * @param shard
   * @param docId
   * @param fields
   * @return
   * @throws IOException
   */
  public MapWritable getDetails(String shard, int docId, String[] fields) throws IOException;

  /**
   * Returns the lucene document. Each field:value tuple of the lucene document
   * is pushed ito the map. In most cases
   * {@link #getDetails(String, int, String[])} would be a better choice for
   * performance reasons.
   * 
   * @param shard
   * @param docId
   * @return
   * @throws IOException
   */
  public MapWritable getDetails(String shard, int docId) throws IOException;

  /**
   * Returns the number of documents that match the given query. This the
   * fastest way in case you just need the number of documents. Note that the
   * number of matching documents is also included in HitsMapWritable.
   * 
   * @param query
   * @param strings
   * @return
   * @throws IOException
   */
  public int getResultCount(IQuery query, String[] strings) throws IOException;
}
