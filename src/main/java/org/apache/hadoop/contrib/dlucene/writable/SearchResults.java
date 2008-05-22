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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Hits;

/**
 * A {@link Writable} wrapper for {@link Hits}.
 */
public class SearchResults implements Writable {

  /** The list of search result values. */
  private List<Document> values = null;

  /**
   * Constructor.
   */
  public SearchResults() {
    values = new ArrayList<Document>();
  }

  /**
   * Constructor.
   * 
   * @param hits the query results
   * @throws IOException
   */
  public SearchResults(Hits hits) throws IOException {
    values = new ArrayList<Document>();
    for (int i = 0; i < hits.length(); i++) {
      values.add(hits.doc(i));
    }
  }

  /**
   * Deserialize search results.
   * 
   * @param in the input stream
   * @return the search results
   * @throws IOException
   */
  public static SearchResults read(DataInput in) throws IOException {
    SearchResults sr = new SearchResults();
    sr.readFields(in);
    return sr;
  }

  /**
   * Add a document.
   * 
   * @param document the document to add
   */
  public void add(Document document) {
    values.add(document);
  }

  /**
   * Combine {@link SearchResults} objects.
   * 
   * @param results the SearchResults
   */
  public void add(SearchResults results) {
    for (int i = 0; i < results.size(); i++) {
      add(results.get(i));
    }
  }

  /**
   * Get the number of results.
   * 
   * @return the size of the results set
   */
  public int size() {
    return values.size();
  }

  /**
   * Get a document.
   * 
   * @param n the number of the document
   * @return the document
   */
  public Document get(int n) {
    return values.get(n);
  }

  /**
   * Enumeration indicating the document field type.
   */
  protected enum FieldType {
    READER, STRING, BINARY, TOKENSTREAM
  };

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
    out.writeInt(values.size()); // write values
    for (Document d : values) {
      WDocument document = new WDocument(d);
      document.write(out);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    int n = in.readInt();
    for (int i = 0; i < n; i++) {
      WDocument document = WDocument.read(in);
      values.add(document.getDocument());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (Document d : values) {
      sb.append(d);
    }
    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((values == null) ? 0 : values.hashCode());
    return result;
  }

  // Lucene does not define equals for Document which makes this a bit tricky
  // this is not complete, needed by unit tests
  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final SearchResults other = (SearchResults) obj;
    if (values == null) {
      if (other.values != null)
        return false;
    }
    // check that documents are the same here
    if (this.size() != other.size()) {
      return false;
    }
    for (int i = 0; i < this.size(); i++) {
      Document d1 = this.get(i);
      Document d2 = other.get(i);
      // Lucene does not define equals for Document which makes this a bit
      // tricky
      List<Field> f1 = d1.getFields();
      List<Field> f2 = d2.getFields();
      if (f1.size() != f2.size()) {
        return false;
      }
      for (int j = 0; j < f1.size(); j++) {
        Field fd1 = f1.get(j);
        Field fd2 = f2.get(j);
        if (fd1.isCompressed() != fd2.isCompressed()
            || fd1.isBinary() != fd2.isBinary()
            || fd1.isStored() != fd2.isStored()
            || fd1.isTokenized() != fd2.isTokenized()
            || fd1.getOmitNorms() != fd2.getOmitNorms()
            || !fd1.stringValue().equals(fd2.stringValue())) {
          return false;
        }
      }
    }
    return true;
  }
}
