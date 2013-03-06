/**
 * Copyright 2011 the original author or authors.
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

import net.sf.katta.util.NodeConfiguration;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

public class DefaultSearcherFactory implements ISeacherFactory {

  @Override
  public void init(NodeConfiguration config) {
    // nothing todo
  }

  @Override
  public IndexHandle createSearcher(String shardName, File shardDir) throws IOException {
    IndexReader indexReader = DirectoryReader.open(FSDirectory.open(shardDir.getAbsoluteFile()));
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
    return new IndexHandle(indexReader, indexSearcher);
  }

  @Override
  public void closeSearcher(IndexHandle indexHandle) throws IOException {
    indexHandle.closeSearcher();
  }

}
