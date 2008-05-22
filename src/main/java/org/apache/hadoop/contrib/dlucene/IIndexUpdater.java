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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

public interface IIndexUpdater {

  /**
   * Add a document to an index.
   * 
   * @param doc the document
   * @return the shard name
   * @throws IOException
   */
  public void addDocument(Document doc) throws IOException;

  /**
   * Remove documents from an index.
   * 
   * @param term the search term
   * @return the number of documents removed
   * @throws IOException
   */
  public int removeDocuments(Term term) throws IOException;

  /**
   * Commit changes to an index.
   * 
   * @throws IOException
   */
  public void commit() throws IOException;

}
