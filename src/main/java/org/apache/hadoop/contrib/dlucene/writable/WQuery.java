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
package org.apache.hadoop.contrib.dlucene.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

/**
 * A {@link Writable} wrapper for {@link Query}.
 */
public class WQuery implements Writable {

  /** The query object being wrapper. */
  private Query query = null;

  /**
   * Constructor.
   * 
   * @param query the query
   */
  public WQuery(Query query) {
    this.query = query;
  }

  /**
   * Constructor.
   */
  private WQuery() {
    // do nothing
  }

  /**
   * Get the Query object being wrapped.
   * 
   * @return the Query object
   */
  public Query getQuery() {
    return query;
  }

  /**
   * Deserialize the Query object.
   * 
   * @param in the input stream
   * @return the Query object
   * @throws IOException
   */
  public static WQuery read(DataInput in) throws IOException {
    WQuery query = new WQuery();
    query.readFields(in);
    return query;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    Utils.checkArgs(out);
    String s = query.toString(null);
    Text.writeString(out, s);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    String q = Text.readString(in);
    QueryParser queryParser = new QueryParser(null, new StandardAnalyzer());
    try {
      this.query = queryParser.parse(q);
    } catch (ParseException p) {
      throw new IOException(p.getMessage());
    }
  }

}
