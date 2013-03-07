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


import net.sf.katta.lib.lucene.query.ILuceneQueryAndFilterWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * The public interface to the back end LuceneServer. These are all the
 * methods that the Hadoop RPC will call.
 */
public interface ILuceneServer extends VersionedProtocol {

  /**
   * @param queryAndFilter         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param timeout       How long the query is allowed to run before getting interrupted
   * @param count         The top n high score hits.
   * @param explainResults true to call Lucene explain method for each hit
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(ILuceneQueryAndFilterWritable queryAndFilter, DocumentFrequencyWritable freqs, String[] shardNames, long timeout, int count, boolean explainResults)
      throws IOException;

  /**
   * Sorts the returned hits based on the sort parameter.
   *
   * @param queryAndFilter         The query to run.
   * @param freqs         Term frequency information for term weighting.
   * @param shardNames    A array of shard names to search in.
   * @param timeout       How long the query is allowed to run before getting interrupted
   * @param count         The top n high score hits.
   * @param sort          sort criteria for returned hits
   * @param explainResults true to call Lucene explain method for each hit
   * @return A list of hits from the search.
   * @throws IOException     If the search had a problem reading files.
   */
  public HitsMapWritable search(ILuceneQueryAndFilterWritable queryAndFilter, DocumentFrequencyWritable freqs, String[] shardNames, long timeout, int count,
      SortWritable sort, boolean explainResults) throws IOException;

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
  public DocumentFrequencyWritable getDocFreqs(ILuceneQueryAndFilterWritable input, String[] shards) throws IOException;

  /**
   * Returns only the requested fields of Lucene documents.  The fields are
   * returned as a map.
   *
   * @param shards       The shards to ask for the document.
   * @param docIdsByShard The documents that are desired, by shard name.
   * @param fields       The fields to return.
   * @return             details of the document
   * @throws IOException
   */
  public MapWritable getDetails(String[] shards, MapWritable docIdsByShard, String[] fields) throws IOException;

  /**
   * Returns the lucene documents. Each field:value tuple of the lucene document
   * is inserted into the returned map. In most cases
   * {@link #getDetails(String[], int[][], String[])} would be a better choice
   * for performance reasons.
   *
   * @param shards        The shards to ask for the document.
   * @param docIdsByShard The documents that are desired, by shard name.
   * @return details of the document
   * @throws IOException
   */
  public MapWritable getDetails(String[] shards, MapWritable docIdsByShard) throws IOException;

  /**
   * Returns the number of documents that match the given query. This the
   * fastest way in case you just need the number of documents. Note that the
   * number of matching documents is also included in HitsMapWritable.
   *
   * @param queryAndFilter
   * @param shards
   * @param timeout How long the query is allowed to run before getting interrupted
   * @return number of documents
   * @throws IOException
   */
  public int getResultCount(ILuceneQueryAndFilterWritable queryAndFilter, String[] shards, long timeout) throws IOException;
}
