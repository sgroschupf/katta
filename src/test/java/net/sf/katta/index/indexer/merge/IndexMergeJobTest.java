package net.sf.katta.index.indexer.merge;

import java.io.File;

import junit.framework.TestCase;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class IndexMergeJobTest extends TestCase {

  private IndexConfiguration _indexConfiguration = new IndexConfiguration();

  @Override
  protected void setUp() throws Exception {
    TestUtil.deleteDirectory(_indexConfiguration.getFile(IndexConfiguration.INDEX_TMP_DIRECTORY));
    TestUtil.deleteDirectory(_indexConfiguration.getFile(IndexConfiguration.MAPRED_OUTPUT_PATH));
  }

  public void testMerging() throws Exception {
    Path[] indexesToMerge = new Path[] { new Path(TestResources.INDEX1.getAbsolutePath()),
        new Path(TestResources.INDEX2.getAbsolutePath()) };
    Path mergedIndexPath = TestUtil.newTestFolder("mergedIndex");
    File mergedIndexFile = new File(mergedIndexPath.toString());
    TestUtil.deleteDirectory(mergedIndexFile);

    IndexMergeJob indexMergeJob = new IndexMergeJob();
    Configuration configuration = new Configuration();
    _indexConfiguration.enrichJobConf(configuration, DfsIndexInputFormat.DOCUMENT_INFORMATION);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.MAPRED_OUTPUT_PATH);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);
    _indexConfiguration.enrichJobConf(configuration, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);
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
