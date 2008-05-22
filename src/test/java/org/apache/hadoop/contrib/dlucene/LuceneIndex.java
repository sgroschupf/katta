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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;

public class LuceneIndex {

  RAMDirectory rd = null;
  
  private Document createDocument(String author, String title, String topic) {

    Document document = new Document();
    document.add(new Field("author", author, Field.Store.YES,
        Field.Index.TOKENIZED));
    document.add(new Field("title", title, Field.Store.YES,
        Field.Index.TOKENIZED));
    document.add(new Field("topic", topic, Field.Store.YES,
        Field.Index.TOKENIZED));
    return document;
  }

  public LuceneIndex() throws IOException {
    rd = new RAMDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(rd, analyzer, true);
    writer.close();
  }

  private void indexDocument(Document document) throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriter writer = new IndexWriter(rd, analyzer, false);
    writer.addDocument(document);
    writer.optimize();
    writer.close();
  }

  public void indexArticle(String author, String title, String topic)
      throws Exception {
    Document document = createDocument(author, title, topic);
    indexDocument(document);
  }

  public Hits search(String searchCriteria) throws IOException, ParseException {
    IndexSearcher is = new IndexSearcher(rd);
    Analyzer analyzer = new StandardAnalyzer();
    QueryParser parser = new QueryParser("article", analyzer);
    Query query = parser.parse(searchCriteria);
    Hits hits = is.search(query);
    return hits;
  }
}
