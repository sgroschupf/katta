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
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.testutil.ExtendedTestCase;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;

public class IndexMergeJobTest extends ExtendedTestCase {

  private static final String SORT_FIELD = "sortField";
  private static final String KEY_FIELD = "keyField";
  private IndexConfiguration _indexConfiguration = new IndexConfiguration();

  @Override
  protected void onSetUp() throws Exception {
    FileUtil.deleteFolder(_indexConfiguration.getFile(IndexConfiguration.INDEX_TMP_DIRECTORY));
    // FileUtil.deleteFolder(_indexConfiguration.getFile(IndexConfiguration.MAPRED_OUTPUT_PATH));
  }

  public void testMerging() throws Exception {
    File index1File = createFile("index1");
    File index2File = createFile("index2");
    Path[] indexesToMerge = new Path[] { new Path(index1File.getAbsolutePath()), new Path(index2File.getAbsolutePath()) };
    writeIndexesToMerge(index1File, index2File);

    Path mergedIndexPath = createPath("mergedIndex");
    File mergedIndexFile = new File(mergedIndexPath.toString());
    FileUtil.deleteFolder(mergedIndexFile);

    Configuration configuration = new Configuration();
    configuration.setInt("io.sort.mb", 20);
    configuration.set(DfsIndexInputFormat.DOCUMENT_INFORMATION, ConfigurableDocumentDuplicateInformation.class
        .getName());
    configuration.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_DOCUMENT_FIELDS, KEY_FIELD + "," + SORT_FIELD);
    configuration.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_KEY_FIELD, KEY_FIELD);
    configuration.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_SORT_FIELD, SORT_FIELD);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);

    IndexMergeJob indexMergeJob = new IndexMergeJob();
    indexMergeJob.setConf(configuration);
    indexMergeJob.merge(indexesToMerge, mergedIndexPath);

    assertEquals(true, mergedIndexFile.exists());
    File indexesFolder = new File(mergedIndexFile, "indexes");
    assertEquals(true, indexesFolder.exists());

    File[] shardZips = indexesFolder.listFiles(FileUtil.VISIBLE_FILES_FILTER);
    assertTrue(shardZips.length > 0);
    assertTrue(shardZips.length < _indexConfiguration.getInt(IndexConfiguration.INDEX_SHARD_COUNT));

    List<Document> mergedDocuments = new ArrayList<Document>();
    for (File shardZip : shardZips) {
      File shardUnzipped = new File(shardZip.getParentFile(), shardZip.getName() + ".unzipped");
      FileUtil.unzip(shardZip, shardUnzipped);
      IndexReader indexReader = IndexReader.open(FSDirectory.getDirectory(shardUnzipped));
      for (int i = 0; i < indexReader.maxDoc(); i++) {
        mergedDocuments.add(indexReader.document(i));
      }
      indexReader.close();
    }
    assertEquals(3, mergedDocuments.size());
    assertEquals("2", getSortValue(mergedDocuments, "key1"));
    assertEquals("2", getSortValue(mergedDocuments, "key2"));
    assertEquals("1", getSortValue(mergedDocuments, "key3"));
  }

  private String getSortValue(List<Document> mergedDocuments, String key) {
    for (Document document : mergedDocuments) {
      if (document.get(KEY_FIELD).equals(key)) {
        return document.get(SORT_FIELD);
      }
    }
    return null;
  }

  private void writeIndexesToMerge(File index1File, File index2File) throws Exception {
    // write index1
    File shard11 = new File(index1File, "0");
    IndexWriter index1Writer = new IndexWriter(FSDirectory.getDirectory(shard11), new StandardAnalyzer());
    addDocument(index1Writer, "key1", "1");
    addDocument(index1Writer, "key2", "1");
    addDocument(index1Writer, "key3", "1");
    index1Writer.close();
    FileUtil.zip(shard11, new File(shard11.getParentFile(), shard11.getName() + ".zip"));
    FileUtil.deleteFolder(shard11);

    // write index2
    File shard21 = new File(index2File, "0");
    IndexWriter index2Writer = new IndexWriter(FSDirectory.getDirectory(shard21), new StandardAnalyzer());
    addDocument(index2Writer, "key1", "2");
    addDocument(index2Writer, "key2", "2");
    index2Writer.close();
    FileUtil.zip(shard21, new File(shard21.getParentFile(), shard21.getName() + ".zip"));
    FileUtil.deleteFolder(shard21);
  }

  private void addDocument(IndexWriter indexWriter, String key, String sortValue) throws CorruptIndexException,
      IOException {
    Document document = new Document();
    document.add(new Field(KEY_FIELD, key, Store.YES, Index.UN_TOKENIZED));
    document.add(new Field(SORT_FIELD, sortValue, Store.YES, Index.UN_TOKENIZED));
    indexWriter.addDocument(document);
  }
}
