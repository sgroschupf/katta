package net.sf.katta.integrationTest;

import java.io.File;
import java.util.Set;

import net.sf.katta.integrationTest.support.AbstractMapFileIntegrationTest;
import net.sf.katta.node.Node;
import net.sf.katta.operation.master.IndexDeployOperation;
import net.sf.katta.operation.master.IndexUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexDeployError.ErrorType;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MasterIntegrationTest extends AbstractMapFileIntegrationTest {

  public MasterIntegrationTest() {
    super(2);
  }

  @Test(timeout = 20000)
  public void testDeployAndUndeployIndex() throws Exception {
    final InteractionProtocol protocol = _miniCluster.getProtocol();

    IndexDeployOperation deployOperation = new IndexDeployOperation(INDEX_NAME, "file://"
            + INDEX_FILE.getAbsolutePath(), getNodeCount());
    protocol.addMasterOperation(deployOperation);

    TestUtil.waitUntilIndexDeployed(protocol, INDEX_NAME);
    assertEquals(1, protocol.getIndices().size());
    IndexMetaData indexMD = protocol.getIndexMD(INDEX_NAME);
    assertEquals(null, indexMD.getDeployError());
    assertEquals(SHARD_COUNT, indexMD.getShards().size());

    Set<Shard> shards = indexMD.getShards();
    for (Shard shard : shards) {
      assertEquals(getNodeCount(), protocol.getShardNodes(shard.getName()).size());
      assertEquals(1, shard.getMetaDataMap().size());
    }

    // undeploy
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(INDEX_NAME);
    protocol.addMasterOperation(undeployOperation);
    TestUtil.waitUntilShardsUndeployed(protocol, indexMD);

    assertEquals(0, protocol.getIndices().size());
    assertEquals(null, protocol.getIndexMD(INDEX_NAME));
    for (Shard shard : shards) {
      assertEquals(0, protocol.getShardNodes(shard.getName()).size());
    }
    assertEquals(0, protocol.getZkClient().getChildren(getZkConfiguration().getZkPath(PathDef.SHARD_TO_NODES)).size());
  }

  @Test(timeout = 20000)
  public void testDeployError() throws Exception {
    final InteractionProtocol protocol = _miniCluster.getProtocol();

    final File indexFile = TestResources.INVALID_INDEX;
    IndexDeployOperation deployOperation = new IndexDeployOperation(INDEX_NAME,
            "file://" + indexFile.getAbsolutePath(), getNodeCount());
    protocol.addMasterOperation(deployOperation);
    TestUtil.waitUntilIndexDeployed(protocol, INDEX_NAME);
    assertEquals(1, protocol.getIndices().size());
    IndexMetaData indexMD = protocol.getIndexMD(INDEX_NAME);
    assertNotNull(indexMD.getDeployError());
    assertEquals(ErrorType.SHARDS_NOT_DEPLOYABLE, indexMD.getDeployError().getErrorType());
  }

  @Test(timeout = 20000)
  public void testRebalanceIndexAfterNodeCrash() throws Exception {
    int replicationCount = getNodeCount() - 1;
    deployTestIndices(1, replicationCount);
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    Thread.sleep(500);
    assertEquals(1, protocol.getIndices().size());

    int optimumShardDeployCount = SHARD_COUNT * replicationCount;
    assertEquals(optimumShardDeployCount, countShardDeployments(protocol, INDEX_NAME));

    _miniCluster.shutdownNode(0);
    assertTrue(optimumShardDeployCount > countShardDeployments(protocol, INDEX_NAME));

    Thread.sleep(2000);
    assertEquals(optimumShardDeployCount, countShardDeployments(protocol, INDEX_NAME));
  }

  @Test(timeout = 20000)
  public void testIndexPickupAfterMasterRestart() throws Exception {
    deployTestIndices(1, getNodeCount());
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    assertEquals(1, protocol.getIndices().size());

    _miniCluster.restartMaster();
    assertEquals(1, protocol.getIndices().size());
    assertTrue(protocol.getReplicationReport(protocol.getIndexMD(INDEX_NAME)).isDeployed());
  }

  @Test
  public void testReplicateUnderreplicatedIndexesAfterNodeAdding() throws Exception {
    int replicationCount = getNodeCount() + 1;
    deployTestIndices(1, replicationCount);
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    assertEquals(1, protocol.getIndices().size());

    int optimumShardDeployCount = SHARD_COUNT * replicationCount;
    assertTrue(optimumShardDeployCount > countShardDeployments(protocol, INDEX_NAME));

    Node node = _miniCluster.startAdditionalNode();
    TestUtil.waitUntilNodeServesShards(protocol, node.getName(), SHARD_COUNT);
    assertTrue(optimumShardDeployCount == countShardDeployments(protocol, INDEX_NAME));
  }
}
