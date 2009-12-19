package net.sf.katta.protocol.operation.node;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class ShardRedeployOperationTest extends AbstractNodeOperationMockTest {

  @Before
  public void setUp() {
    when(_shardManager.getShardFolder((String) notNull())).thenReturn(new File("shardFolder"));
  }

  @Test
  public void testRedeploy() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");
    ShardRedeployOperation operation = new ShardRedeployOperation(shards);
    operation.execute(_context);

    InOrder inOrder = inOrder(_protocol, _nodeManaged);
    for (String shard : shards) {
      inOrder.verify(_nodeManaged).addShard(eq(shard), (File) notNull());
      inOrder.verify(_protocol).publishShard(eq(_node), eq(shard), (Map<String, String>) notNull());
    }
  }

  @Test
  public void testRedeployShardAlreadyKnownToNodeManaged() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");

    when(_nodeManaged.getShards()).thenReturn(shards);
    ShardRedeployOperation operation = new ShardRedeployOperation(shards);
    operation.execute(_context);

    // only publis but not add to nodemanaged again
    InOrder inOrder = inOrder(_protocol, _nodeManaged);
    for (String shard : shards) {
      inOrder.verify(_protocol).publishShard(eq(_node), eq(shard), (Map<String, String>) notNull());
    }
  }
}
