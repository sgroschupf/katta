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

import net.sf.katta.testutil.ExtendedTestCase;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class IndexMergeJobTest extends ExtendedTestCase {

  private IndexConfiguration _indexConfiguration = new IndexConfiguration();

  @Override
  protected void onSetUp() throws Exception {
    FileUtil.deleteFolder(_indexConfiguration.getFile(IndexConfiguration.INDEX_TMP_DIRECTORY));
    FileUtil.deleteFolder(_indexConfiguration.getFile(IndexConfiguration.MAPRED_OUTPUT_PATH));
  }

  public void testMerging() throws Exception {
    Path[] indexesToMerge = new Path[] { new Path(TestResources.INDEX1.getAbsolutePath()),
        new Path(TestResources.INDEX2.getAbsolutePath()) };
    Path mergedIndexPath = createPath("mergedIndex");
    File mergedIndexFile = new File(mergedIndexPath.toString());
    FileUtil.deleteFolder(mergedIndexFile);

    IndexMergeJob indexMergeJob = new IndexMergeJob();
    Configuration configuration = new Configuration();
    _indexConfiguration.enrichJobConf(configuration, DfsIndexInputFormat.DOCUMENT_INFORMATION);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.MAPRED_OUTPUT_PATH);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);
    configuration.setInt("io.sort.mb", 20);
    indexMergeJob.setConf(configuration);
    indexMergeJob.merge(indexesToMerge, mergedIndexPath);

    assertEquals(true, mergedIndexFile.exists());
    File indexesFolder = new File(mergedIndexFile, "indexes");
    assertEquals(true, indexesFolder.exists());

    String[] shardCount = indexesFolder.list(FileUtil.VISIBLE_FILES_FILTER);
    assertTrue(shardCount.length > 0);
    assertTrue(shardCount.length < _indexConfiguration.getInt(IndexConfiguration.INDEX_SHARD_COUNT));

    // TODO jz: compare document count of merged and unmerged indexes
  }
}
