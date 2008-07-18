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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.katta.index.indexer.IDocumentFactory;
import net.sf.katta.util.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

public class DfsIndexDocumentFactory implements IDocumentFactory<Text, DocumentInformation> {


  private FileSystem _fileSystem;

  private Map<Text, IndexReader> _readerMap = new HashMap<Text, IndexReader>();

  public Document convert(Text key, DocumentInformation value) {

    Document document = null;
    IntWritable docId = value.getDocId();
    Text text = value.getIndexPath();
    try {

      if (!_readerMap.containsKey(text)) {
        IndexReader reader = IndexReader.open(new DfsIndexDirectory(_fileSystem, null, new Path(text.toString())));
        _readerMap.put(text, reader);
      }
      IndexReader reader = _readerMap.get(text);
      document = reader.document(docId.get());
    } catch (Exception e) {
      Logger.warn("can not read document '" + docId + "'from index '" + text + "'", e);
    }
    return document;
  }

  public Analyzer getIndexAnalyzer() {
    return new StandardAnalyzer();
  }

  public void configure(JobConf jobConf) throws IOException {
    _fileSystem = FileSystem.get(jobConf);
  }
}
