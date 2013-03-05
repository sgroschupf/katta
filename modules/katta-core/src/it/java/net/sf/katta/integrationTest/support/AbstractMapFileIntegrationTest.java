package net.sf.katta.integrationTest.support;

import net.sf.katta.lib.mapfile.MapFileServer;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;

import java.io.File;

public abstract class AbstractMapFileIntegrationTest extends AbstractIntegrationTest {
  protected static final File INDEX_FILE = TestResources.MAP_FILE_A;
  protected static final String INDEX_NAME = TestResources.MAP_FILE_A.getName() + 0;

  protected final static int SHARD_COUNT = INDEX_FILE.list(FileUtil.VISIBLE_FILES_FILTER).length;

  public AbstractMapFileIntegrationTest(int nodeCount) {
    this(nodeCount, false, true);
  }

  public AbstractMapFileIntegrationTest(int nodeCount, boolean shutdownAfterEachTest,
                                        boolean undeployIndicesAfterEachTest) {
    super(INDEX_NAME, INDEX_FILE, MapFileServer.class, nodeCount, shutdownAfterEachTest, undeployIndicesAfterEachTest);
  }
}
