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
package net.sf.katta.index.indexer.merge;

import java.io.File;
import java.io.IOException;

import net.sf.katta.testutil.ExtendedTestCase;
import net.sf.katta.testutil.TestResources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

public class DfsIndexInputTest extends ExtendedTestCase {

  private File _file = createFile(getClass().getSimpleName());

  public void testReadIndex() throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);
    Directory directory = new DfsIndexDirectory(fileSystem, new Path(TestResources.SHARD1.getAbsolutePath()), new Path(
        _file.getAbsolutePath()));
    IndexReader reader = IndexReader.open(directory);
    int maxDocs = reader.maxDoc();
    assertEquals(2, maxDocs);
    for (int i = 0; i < maxDocs; i++) {
      Document document = reader.document(i);
      assertNotNull(document.getField("foo"));
      assertTrue(document.get("foo").startsWith("bar"));
    }
  }
}
