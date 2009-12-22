package net.sf.katta.protocol.operation.leader;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.operation.node.NodeOperation;

import org.junit.Test;

public class CheckIndicesOperationTest extends MockedMasterNodeTest {

  @Test
  public void testBalanceUnderreplicatedIndex() throws Exception {
    OperationQueue<LeaderOperation> masterQueue = publishMaster();

    // add nodes and index
    List<Node> nodes = mockNodes(2);
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    deployIndex(nodes, nodeQueues);
    assertEquals(0, masterQueue.size());

    // balance the index does not change anything
    CheckIndicesOperation checkOperation = new CheckIndicesOperation();
    checkOperation.execute(_context, EMPTY_LIST);
    assertEquals(0, masterQueue.size());

    // add node and then balance again
    Node node3 = mockNode();
    publisNode(node3);
    checkOperation.execute(_context, EMPTY_LIST);
    assertEquals(1, masterQueue.size());
  }

  @Test
  public void testBalanceOverreplicatedIndex() throws Exception {
    OperationQueue<LeaderOperation> masterQueue = publishMaster();

    // add nodes and index
    List<Node> nodes = mockNodes(3);
    List<OperationQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    deployIndex(nodes, nodeQueues);
    assertEquals(0, masterQueue.size());

    // balance the index does not change anything
    CheckIndicesOperation balanceOperation = new CheckIndicesOperation();
    balanceOperation.execute(_context, EMPTY_LIST);
    assertEquals(0, masterQueue.size());

    // decrease the replication count and then balance again
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    indexMD.setReplicationLevel(2);
    _protocol.updateIndexMD(indexMD);
    balanceOperation.execute(_context, EMPTY_LIST);
    assertEquals(1, masterQueue.size());
  }

}