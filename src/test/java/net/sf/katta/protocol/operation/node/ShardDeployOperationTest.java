package net.sf.katta.protocol.operation.node;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.InOrder;

public class ShardDeployOperationTest extends AbstractNodeOperationMockTest {

  @Test
  public void testDeploy() throws Exception {
    ShardDeployOperation operation = new ShardDeployOperation();
    operation.addShard("shard1", "shardPath1");
    operation.addShard("shard2", "shardPath2");

    File shardFolder = new File("shardFolder");
    Map<String, String> shardMD = new HashMap<String, String>();
    when(_shardManager.installShard((String) notNull(), (String) notNull())).thenReturn(shardFolder);
    when(_nodeManaged.getShardMetaData((String) notNull())).thenReturn(shardMD);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _nodeManaged);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_shardManager).installShard(shard, operation.getShardPath(shard));
      inOrder.verify(_nodeManaged).addShard(shard, shardFolder);
      inOrder.verify(_protocol).publishShard(_node, shard, shardMD);
    }
    assertEquals(0, result.getShardExceptions().size());
  }

  @Test
  public void testDeployWithOneFailingShard() throws Exception {
    ShardDeployOperation operation = new ShardDeployOperation();
    operation.addShard("shard1", "shardPath1");
    operation.addShard("shard2", "shardPath2");

    File shardFolder = new File("shardFolder");
    Map<String, String> shardMD = new HashMap<String, String>();
    when(_shardManager.installShard((String) notNull(), (String) notNull())).thenReturn(shardFolder);
    when(_nodeManaged.getShardMetaData((String) notNull())).thenReturn(shardMD);

    String failingShard = operation.getShardNames().iterator().next();
    doThrow(new Exception("testException")).when(_nodeManaged).addShard(failingShard, shardFolder);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _nodeManaged);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_shardManager).installShard(shard, operation.getShardPath(shard));
      inOrder.verify(_nodeManaged).addShard(shard, shardFolder);
      if (!shard.equals(failingShard)) {
        inOrder.verify(_protocol).publishShard(_node, shard, shardMD);
      }
    }
    assertEquals(1, result.getShardExceptions().size());
    assertEquals(failingShard, result.getShardExceptions().iterator().next().getKey());
    verify(_shardManager).uninstallShard(eq(failingShard));
  }
}
