package net.sf.katta.integrationTest.support;

import java.io.File;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;

import org.junit.After;

public abstract class AbstractIntegrationTest {

  // TODO let the cluster run, but cleanup indices ?
  protected KattaMiniCluster _miniCluster;
  protected final static File INDEX_FILE = TestResources.INDEX1;
  protected final static String INDEX_NAME = TestResources.INDEX1.getName() + 0;
  protected final static int SHARD_COUNT = INDEX_FILE.list(FileUtil.VISIBLE_FILES_FILTER).length;

  @After
  public void tearDown() throws Exception {
    if (_miniCluster != null) {
      _miniCluster.stop();
    }
  }

  protected KattaMiniCluster startMiniCluster(int nodeCount, int indexCount, int replicationCount) throws Exception {
    ZkConfiguration conf = new ZkConfiguration();
    FileUtil.deleteFolder(new File(conf.getZKDataDir()));
    FileUtil.deleteFolder(new File(conf.getZKDataLogDir()));
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());

    // start katta cluster
    KattaMiniCluster miniCluster = new KattaMiniCluster(conf, nodeCount);
    miniCluster.start();

    miniCluster.deployTestIndexes(INDEX_FILE, indexCount, replicationCount);
    return miniCluster;
  }

  protected int countShardDeployments(InteractionProtocol protocol, String indexName) {
    IndexMetaData indexMD = protocol.getIndexMD(indexName);
    int shardDeployCount = 0;
    for (Shard shard : indexMD.getShards()) {
      shardDeployCount += protocol.getShardNodes(shard.getName()).size();
    }
    return shardDeployCount;
  }

}
