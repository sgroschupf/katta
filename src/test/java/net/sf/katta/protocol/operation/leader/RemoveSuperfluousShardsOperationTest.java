package net.sf.katta.protocol.operation.leader;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.ShardUndeployOperation;

import org.junit.Test;

public class RemoveSuperfluousShardsOperationTest extends MockedMasterNodeTest {

  @Test
  public void testRemove() throws Exception {
    Node node = mockNode();
    OperationQueue<NodeOperation> nodeQueue = publisNode(node);

    // add some valid shards
    deployIndex(Arrays.asList(node), Arrays.asList(nodeQueue));

    // add shard without index
    String someOldShard = AbstractIndexOperation.createShardName("someOldIndex", "someOldShard");
    _protocol.publishShard(node, someOldShard, new HashMap<String, String>());

    // execute
    assertEquals(0, nodeQueue.size());
    RemoveSuperfluousShardsOperation operation = new RemoveSuperfluousShardsOperation(node.getName());
    operation.execute(_context);
    assertEquals(1, nodeQueue.size());
    ShardUndeployOperation undeployOperation = (ShardUndeployOperation) nodeQueue.peek();
    assertEquals(1, undeployOperation.getShardNames().size());
    assertEquals(someOldShard, undeployOperation.getShardNames().iterator().next());
  }
}
