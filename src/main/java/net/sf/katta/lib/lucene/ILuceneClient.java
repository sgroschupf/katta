/**
 * Copyright 2008 the original author or authors.
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

import java.util.List;

import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

/**
 * Client for searching document indices deployed on a katta cluster.
 * <p>
 * 
 * You provide a {@link Query} and the name of the deployed indices, and get
 * back {@link Hits} which contains multiple {@link Hit} objects as the results.
 * <br>
 * See {@link #search(Query, String[], int)}.
 * <p>
 * 
 * The details of a hit-document can be retrieved through the
 * {@link #getDetails(Hit, String[])} method.
 * 
 * @see Hit
 * @see Hits
 * @see Query
 * @see Sort
 */
public interface ILuceneClient {

  /**
   * Searches with a given query in the supplied indexes for an almost unlimited
   * ({@link Integer#MAX_VALUE}) amount of results.
   * 
   * If this method might has poor performance try to limit results with
   * {@link #search(IQuery, String[], int)}.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @return A object that capsulates all results.
   * @throws KattaException
   */
  public Hits search(Query query, String[] indexNames) throws KattaException;

  /**
   * Searches with a given query in the supplied indexes for a limited amount of
   * results.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @param count
   *          The count of results that should be returned.
   * @return A object that capsulates all results.
   * @throws KattaException
   */
  public Hits search(Query query, String[] indexNames, int count) throws KattaException;

  /**
   * Searches with a given query in the supplied indexes for a limited amount of
   * results and sorts the results based upon the sort parameter.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @param count
   *          The count of results that should be returned.
   * @param sort
   *          Sort criteria for returned hits
   * @return A object that capsulates all results.
   * @throws KattaException
   */
  public Hits search(Query query, String[] indexNames, int count, Sort sort) throws KattaException;

  /**
   * Gets all the details to a hit.
   * 
   * @param hit
   *          The {@link Hit} from that all fields should be returned.
   * @return All fields to a {@link Hit} as field name and field value pairs.
   * @throws KattaException
   *           If indexes can't be searched.
   */
  public MapWritable getDetails(Hit hit) throws KattaException;

  /**
   * Gets a specific details to a hit.
   * 
   * @param hit
   *          The {@link Hit} from that all fields should be returned.
   * @param fields
   *          The names of the fields from that the value should be returned.
   * @return The supplied field to a {@link Hit} as field name and field value
   *         pair.
   * @throws KattaException
   *           If indexes can't be searched.
   */
  public MapWritable getDetails(Hit hit, String[] fields) throws KattaException;

  /**
   * Gets list of all details for the given list of hits. The details are
   * retrieved in parallel rather than getting them one by one. Thus using this
   * method is the preferred way of getting the details of multiple hits.
   * 
   * @param hits
   *          The list of hits from that all fields should be returned.
   * @return The list of details for given hits.
   * @throws KattaException
   *           If indexes can't be searched.
   * @throws InterruptedException
   *           If the current thread got interrupted.
   */
  public List<MapWritable> getDetails(List<Hit> hits) throws KattaException, InterruptedException;

  /**
   * Gets list of details for the given list of hits. The details are retrieved
   * in parallel rather than getting them one by one. Thus using this method is
   * the preferred way of getting the details of multiple hits.
   * 
   * @param hits
   *          The list of hits from that all fields should be returned.
   * @param fields
   *          The field names of which the value should be returned.
   * @return The list of details for given hits.
   * @throws KattaException
   *           If indexes can't be searched.
   * @throws InterruptedException
   *           If the current thread got interrupted.
   */
  public List<MapWritable> getDetails(List<Hit> hits, final String[] fields) throws KattaException,
          InterruptedException;

  /**
   * The overall queries per minute.
   * 
   * @return A number that represents the queries per minute in the last minute.
   */
  public double getQueryPerMinute();

  /**
   * Gets only the result count to a query.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @return A number that represents the overall result count to a query.
   * @throws KattaException
   */
  public int count(Query query, String[] indexNames) throws KattaException;

  /**
   * Closes down the client.
   */
  public void close();

}