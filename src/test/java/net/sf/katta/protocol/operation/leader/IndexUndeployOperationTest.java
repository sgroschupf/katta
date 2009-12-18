package net.sf.katta.protocol.operation.leader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.ShardUndeployInstruction;

import org.junit.Test;

public class IndexUndeployOperationTest extends MockedMasterNodeTest {

  @Test
  public void testUndeployIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = mockNodes(2);
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    deployIndex(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNull(_protocol.getIndexError(_indexName));

    // balance the index does not change anything
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context);
    for (OperationQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployInstruction);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertNull(_protocol.getIndexError(_indexName));
  }

  @Test
  public void testUndeployErrorIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = mockNodes(2);
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    deployIndexWithError();
    publisShards(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNotNull(_protocol.getIndexError(_indexName));

    // balance the index does not change anything
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context);
    for (OperationQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployInstruction);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertNull(_protocol.getIndexError(_indexName));
  }

}
