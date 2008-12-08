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
package net.sf.katta.testutil;

import java.io.IOException;
import java.util.Set;

import net.sf.katta.index.indexer.IDocumentFactory;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

public class TestDocumentFactory implements IDocumentFactory<WritableComparable<?>, MapWritable> {

  public void configure(JobConf jobConf) throws IOException {
    //
  }

  public Document convert(WritableComparable<?> key, MapWritable mapWritable) {
    final Document document = new Document();
    final Field id = new Field("id", key.toString(), Store.YES, Index.UN_TOKENIZED);
    document.add(id);
    Set<Writable> keys = mapWritable.keySet();
    for (Writable mapKey : keys) {
      Field mapField = new Field(mapKey.toString(), mapWritable.get(mapKey).toString(), Store.YES, Index.TOKENIZED);
      document.add(mapField);
    }
    return document;
  }

  public Analyzer getIndexAnalyzer() {
    return new StandardAnalyzer();
  }

}
