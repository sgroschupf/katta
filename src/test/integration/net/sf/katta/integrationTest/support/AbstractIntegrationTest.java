package net.sf.katta.integrationTest.support;

import java.io.File;
import java.util.List;

import net.sf.katta.AbstractTest;
import net.sf.katta.client.DeployClient;
import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.node.IContentServer;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

/**
 * Test which starts a katta mini cluster for the test class. A test class can
 * decide how many nodes should run and if the cluster is restarted between each
 * test method. If not restarted, the deployed indices are removed between test
 * methods. Also nodes are restarted if shut down during a test.
 */
public abstract class AbstractIntegrationTest extends AbstractTest {

  final static Logger LOG = Logger.getLogger(AbstractIntegrationTest.class);

  protected final static File INDEX_FILE = TestResources.INDEX1;
  protected final static String INDEX_NAME = TestResources.INDEX1.getName() + 0;
  protected final static int SHARD_COUNT = INDEX_FILE.list(FileUtil.VISIBLE_FILES_FILTER).length;

  protected static KattaMiniCluster _miniCluster;
  protected static InteractionProtocol _protocol;
  private static int _lastNodeStartPort = 20000;

  private final int _nodeCount;
  private final boolean _shutdownAfterEachTest;
  private final boolean _undeployIndicesAfterEachTest;

  private final Class<? extends IContentServer> _contentServerClass;

  public AbstractIntegrationTest(int nodeCount) {
    this(LuceneServer.class, nodeCount);
  }

  public AbstractIntegrationTest(Class<? extends IContentServer> nodeServerClass, int nodeCount) {
    this(nodeServerClass, nodeCount, false, true);
  }

  public AbstractIntegrationTest(int nodeCount, boolean shutdownAfterEachTest, boolean undeployIndicesAfterEachTest) {
    this(LuceneServer.class, nodeCount, shutdownAfterEachTest, undeployIndicesAfterEachTest);
  }

  public AbstractIntegrationTest(Class<? extends IContentServer> nodeServerClass, int nodeCount,
          boolean shutdownAfterEachTest, boolean undeployIndicesAfterEachTest) {
    _contentServerClass = nodeServerClass;
    _nodeCount = nodeCount;
    _shutdownAfterEachTest = shutdownAfterEachTest;
    _undeployIndicesAfterEachTest = undeployIndicesAfterEachTest;
  }

  @AfterClass
  public final static void tearDownClass() throws Exception {
    if (_miniCluster != null) {
      _miniCluster.stop();
      _miniCluster = null;
    }
  }

  @After
  public final void tearDown() throws Exception {
    if (_miniCluster != null) {
      _lastNodeStartPort += _miniCluster.getStartedNodeCount();
    } else {
      _lastNodeStartPort += _nodeCount;
    }
    if (_shutdownAfterEachTest && _miniCluster != null) {
      _miniCluster.stop();
      _miniCluster = null;
    }
  }

  @Before
  public final void setUp() throws Exception {
    LOG.info("~~~~~~~~~~~~~~~~~~" + "SETUP CLUSTER" + "~~~~~~~~~~~~~~~~~~");
    if (_miniCluster == null) {
      startMiniCluster(_nodeCount, 0, 0);
      afterClusterStart();
    } else if (!_shutdownAfterEachTest) {
      // restart nodes
      LOG.info("nodes " + _miniCluster.getNodes() + " running.");
      int numberOfRunningNode = _miniCluster.getRunningNodeCount();
      int nodesToStop = numberOfRunningNode - _nodeCount;
      int nodesToStart = _nodeCount - numberOfRunningNode;
      if (nodesToStop > 0) {
        for (int i = 0; i < nodesToStop; i++) {
          Node node = _miniCluster.shutdownNode(i);
          LOG.info("stopped " + node.getName());
        }
      } else if (nodesToStart > 0) {
        for (int i = 0; i < nodesToStart; i++) {
          Node node = _miniCluster.startAdditionalNode();
          LOG.info("started " + node.getName());
        }
      }
      TestUtil.waitUntilNumberOfLiveNode(_protocol, _nodeCount);
      // remove all indices
      if (_undeployIndicesAfterEachTest) {
        List<String> indices = _protocol.getIndices();
        for (String index : indices) {
          IndexMetaData indexMD = _protocol.getIndexMD(index);
          DeployClient deployClient = new DeployClient(_protocol);
          deployClient.removeIndex(index);
          TestUtil.waitUntilShardsUndeployed(_protocol, indexMD);
        }
      } else {
        List<String> indices = _protocol.getIndices();
        for (String index : indices) {
          TestUtil.waitUntilIndexBalanced(_protocol, index);
        }
      }
      LOG.info("~~~~~~~~~~~~~~~~~~" + "FIN SETUP CLUSTER" + "~~~~~~~~~~~~~~~~~~");
    }
  }

  protected void afterClusterStart() throws Exception {
    // subclasses may override
  }

  public int getNodeCount() {
    return _nodeCount;
  }

  private final KattaMiniCluster startMiniCluster(int nodeCount, int indexCount, int replicationCount) throws Exception {
    ZkConfiguration conf = new ZkConfiguration();
    FileUtil.deleteFolder(new File(conf.getZKDataDir()));
    FileUtil.deleteFolder(new File(conf.getZKDataLogDir()));
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());

    // start katta cluster
    _miniCluster = new KattaMiniCluster(_contentServerClass, conf, nodeCount, _lastNodeStartPort);
    // we permanently start the node on other
    // ports because hadoop rpc seems to make
    // some trouble if a rpc server is restarted on the same port immediately
    _miniCluster.start();

    deployTestIndices(indexCount, replicationCount);
    _protocol = _miniCluster.getProtocol();
    return _miniCluster;
  }

  protected void shutdownCluster() {
    _miniCluster.stop();
    _miniCluster = null;
  }

  protected void shutdownNodes() {
    for (int i = 0; i < _miniCluster.getRunningNodeCount(); i++) {
      _miniCluster.shutdownNode(i);
    }
  }

  protected void deployTestIndices(int indexCount, int replicationCount) throws InterruptedException {
    _miniCluster.deployTestIndexes(INDEX_FILE, indexCount, replicationCount);
  }

  protected final int countShardDeployments(InteractionProtocol protocol, String indexName) {
    IndexMetaData indexMD = protocol.getIndexMD(indexName);
    int shardDeployCount = 0;
    for (Shard shard : indexMD.getShards()) {
      shardDeployCount += protocol.getShardNodes(shard.getName()).size();
    }
    return shardDeployCount;
  }

}
