/**
 * Copyright 2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.lib.lucene;

import java.io.IOException;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * The public interface to the back end LuceneServer. These are all the
 * methods that the Hadoop RPC will call.
 */
public interface ILuceneServer extends VersionedProtocol {

  /**
   * Returns all Hits that match the query. This might be significant slower as
   * {@link #search(QueryWritable, DocumentFrequencyWritable , String[], long, int)} since we
   * replace count with Integer.MAX_VALUE.
   *
   * @param query         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param timeout       How long the query is allowed to run before getting interrupted
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shardNames, long timeout) throws IOException;


  /**
   * @param query         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param timeout       How long the query is allowed to run before getting interrupted
   * @param count         The top n high score hits.
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shardNames, long timeout, int count)
      throws IOException;

  /**
   * Sorts the returned hits based on the sort parameter.
   *
   * @param query         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param timeout       How long the query is allowed to run before getting interrupted
   * @param count         The top n high score hits.
   * @param sort          sort criteria for returned hits
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(QueryWritable query, DocumentFrequencyWritable freqs, String[] shardNames, long timeout, int count,
      SortWritable sort) throws IOException;

  /**
   * Returns the number of documents a term occurs in. In a distributed search
   * environment, we need to get this first and then query all nodes again with
   * this information to ensure we compute TF IDF correctly. See
   * <a href="http://lucene.apache.org/java/2_3_0/api/org/apache/lucene/search/Similarity.html">Lucene's Similarity</a>
   *
   * @param input       TODO is this really just a Lucene query?
   * @param shards      The shards to search in.
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public DocumentFrequencyWritable getDocFreqs(QueryWritable input, String[] shards) throws IOException;

  /**
   * Returns only the requested fields of a lucene document.  The fields are returned
   * as a map.
   *
   * @param shards       The shards to ask for the document.
   * @param docId        The document that is desired.
   * @param fields       The fields to return.
   * @return             details of the document
   * @throws IOException
   */
  public MapWritable getDetails(String[] shards, int docId, String[] fields) throws IOException;

  /**
   * Returns the lucene document. Each field:value tuple of the lucene document
   * is inserted into the returned map. In most cases
   * {@link #getDetails(String[], int, String[])} would be a better choice for
   * performance reasons.
   *
   * @param shards       The shards to ask for the document.
   * @param docId        The document that is desired.
   * @return details of the document
   * @throws IOException
   */
  public MapWritable getDetails(String[] shards, int docId) throws IOException;

  /**
   * Returns the number of documents that match the given query. This the
   * fastest way in case you just need the number of documents. Note that the
   * number of matching documents is also included in HitsMapWritable.
   *
   * @param query
   * @param shards
   * @param timout How long the query is allowed to run before getting interrupted
   * @return number of documents
   * @throws IOException
   */
  public int getResultCount(QueryWritable query, String[] shards, long timout) throws IOException;
}
