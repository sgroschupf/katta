package net.sf.katta.integrationTest.support;

import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;

import java.io.File;

public abstract class AbstractLuceneIntegrationTest extends AbstractIntegrationTest {
  protected static final File INDEX_FILE = TestResources.LUCENE_4_INDEX1;
  protected static final String INDEX_NAME = TestResources.LUCENE_4_INDEX1.getName() + 0;

  protected final static int SHARD_COUNT = INDEX_FILE.list(FileUtil.VISIBLE_FILES_FILTER).length;

  public AbstractLuceneIntegrationTest(int nodeCount) {
    this(nodeCount, false, true);
  }

  public AbstractLuceneIntegrationTest(int nodeCount, boolean shutdownAfterEachTest,
                                       boolean undeployIndicesAfterEachTest) {
    super(INDEX_NAME, INDEX_FILE, LuceneServer.class, nodeCount, shutdownAfterEachTest, undeployIndicesAfterEachTest);
  }
}
