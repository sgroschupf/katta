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
package net.sf.katta.client;

import java.io.IOException;

import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.IQuery;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;

/**
 * Client for searching document indices deployed on a katta cluster.
 * <p>
 * 
 * You provide a {@link IQuery} and the name of the deployed indices, and get
 * back {@link Hits} which contains multiple {@link Hit} objects as the results.
 * <br>
 * See {@link #search(IQuery, String[], int)}.
 * <p>
 * 
 * The details of a hit-document can be retrieved through the
 * {@link #getDetails(Hit, String[])} method.
 * 
 * @see Hit
 * @see Hits
 * @see IQuery
 */
public interface IClient {

  /**
   * Searches with a given query in the supplied indexes for an almost unlimited
   * ({@link Integer.MAX_VALUE}) amount of results.
   * 
   * If this method might has poor performance try to limit results with
   * {@link #search(IQuery, String[], int)}.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @return A object that capsulates all results.
   * @throws IOException
   *           If indexes can't be searched.
   * @throws KattaException
   */
  public abstract Hits search(IQuery query, String[] indexNames) throws KattaException;

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
   * @throws IOException
   *           If indexes can't be searched.
   * @throws KattaException
   */
  public abstract Hits search(IQuery query, String[] indexNames, int count) throws KattaException;

  /**
   * Gets all the details to a hit.
   * 
   * @param hit
   *          The {@link Hit} from that all fields should be returned.
   * @return All fields to a {@link Hit} as field name and field value pairs.
   * @throws IOException
   *           If indexes can't be searched.
   */
  public abstract MapWritable getDetails(Hit hit) throws IOException;

  /**
   * Gets a specific details to a hit.
   * 
   * @param hit
   *          The {@link Hit} from that all fields should be returned.
   * @param fields
   *          The names of the fields from that the value should be returned.
   * @return The supplied field to a {@link Hit} as field name and field value
   *         pair.
   * @throws IOException
   *           If indexes can't be searched.
   */
  public abstract MapWritable getDetails(Hit hit, String[] fields) throws IOException;

  /**
   * The overall queries per minute.
   * 
   * @return A number that represents the queries per minute in the last minute.
   */
  public abstract float getQueryPerMinute();

  /**
   * Gets only the result count to a query.
   * 
   * @param query
   *          The query to search with.
   * @param indexNames
   *          A list of index names to search in.
   * @return A number that represents the overall result count to a query.
   */
  public abstract int count(IQuery query, String[] indexNames);

  /**
   * Closes down the client.
   */
  public abstract void close();

}