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
package net.sf.katta.client;

import java.io.IOException;

import net.sf.katta.slave.Hit;
import net.sf.katta.slave.Hits;
import net.sf.katta.slave.IQuery;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.MapWritable;
import org.apache.lucene.analysis.Analyzer;

public interface IClient {

  /**
   * Don't use this method instead use {@link #search(IQuery, String[], int)}
   * since we set the max result count to {@link Integer.MAX_VALUE}.
   * 
   * @param query
   *            The query to search with.
   * @param indexNames
   *            A list of index names to search in.
   * @return A object that capsulates all results.
   * @throws IOException
   *             If indexes can't be searched.
   * @throws KattaException
   */
  public abstract Hits search(IQuery query, String[] indexNames) throws KattaException;

  /**
   * Searches for a given query in the supplied indexes.
   * 
   * @param query
   *            The query to search with.
   * @param indexNames
   *            A list of index names to search in.
   * @param count
   *            The count of results that should be returned.
   * @return A object that capsulates all results.
   * @throws IOException
   *             If indexes can't be searched.
   * @throws KattaException
   */
  public abstract Hits search(IQuery query, String[] indexNames, int count) throws KattaException;

  /**
   * Gets all the details to a hit.
   * 
   * @param hit
   *            The {@link Hit} from that all fields should be returned.
   * @return All fields to a {@link Hit} as field name and field value pairs.
   * @throws IOException
   *             If indexes can't be searched.
   */
  public abstract MapWritable getDetails(Hit hit) throws IOException;

  /**
   * Gets a specific details to a hit.
   * 
   * @param hit
   *            The {@link Hit} from that all fields should be returned.
   * @param fields
   *            The names of the fields from that the value should be returned.
   * @return The supplied field to a {@link Hit} as field name and field value
   *         pair.
   * @throws IOException
   *             If indexes can't be searched.
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
   *            The query to search with.
   * @param indexNames
   *            A list of index names to search in.
   * @return A number that represents the overall result count to a query.
   */
  public abstract int count(IQuery query, String[] indexNames);

  /**
   * Adds a index to the Katta system. The index path is a URI that can point to
   * a local filesystem or to a Hadoop DFS.
   * 
   * @param indexName
   *            Name that represents the new index.
   * @param pathToIndex
   *            URI to the index. Should start with file:// or hdfs://.
   * @param analyzer
   *            The Lucene analyzer class name the index was created with.
   * @throws KattaException
   */
  public abstract void addIndex(String indexName, String pathToIndex, Analyzer analyzer) throws KattaException;

}