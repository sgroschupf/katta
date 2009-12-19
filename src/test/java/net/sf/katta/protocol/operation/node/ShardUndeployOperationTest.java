package net.sf.katta.protocol.operation.node;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.InOrder;

public class ShardUndeployOperationTest extends AbstractNodeOperationMockTest {

  @Test
  public void testUneploy() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");
    ShardUndeployOperation operation = new ShardUndeployOperation(shards);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _nodeManaged);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_protocol).unpublishShard(_node, shard);
      inOrder.verify(_nodeManaged).removeShard(shard);
      inOrder.verify(_shardManager).uninstallShard(shard);
    }
    assertEquals(0, result.getShardExceptions().size());
  }

  @Test
  public void testUneployWithError() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");
    ShardUndeployOperation operation = new ShardUndeployOperation(shards);

    String failingShard = shards.get(0);
    doThrow(new Exception("testException")).when(_nodeManaged).removeShard(failingShard);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _nodeManaged);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_protocol).unpublishShard(_node, shard);
      inOrder.verify(_nodeManaged).removeShard(shard);
      inOrder.verify(_shardManager).uninstallShard(shard);
    }
    assertEquals(1, result.getShardExceptions().size());
  }

}
