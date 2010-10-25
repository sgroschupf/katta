/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.operation.master;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.node.Node;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation.ExecutionInstruction;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.testutil.Mocks;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import static org.mockito.Matchers.any;

@SuppressWarnings("unchecked")
public class BalanceIndexOperationTest extends AbstractMasterNodeZkTest {

  @Test
  public void testGetExecutionInstruction() throws Exception {
    // only lock operations on same index
    MasterOperation op1 = new BalanceIndexOperation("index1");
    MasterOperation op2 = new BalanceIndexOperation("index1");

    assertEquals(ExecutionInstruction.EXECUTE, op1.getExecutionInstruction(EMPTY_LIST));
    assertEquals(ExecutionInstruction.CANCEL, op2.getExecutionInstruction(Arrays.asList(op1)));
  }

  @Test
  public void testBalanceUnderreplicatedIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();

    // index deployed on 2 nodes / desired replica is 3
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    publisShards(nodes, nodeQueues);

    // balance the index does not change anything
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    balanceOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(0, nodeqQueue.size());
    }

    // add node and then balance again
    Node node3 = Mocks.mockNode();
    NodeQueue nodeQueue3 = Mocks.publisNode(_protocol, node3);
    assertEquals(0, nodeQueue3.size());

    balanceOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(0, nodeqQueue.size());
    }
    assertEquals(1, nodeQueue3.size());
  }

  @Test
  public void testBalanceOverreplicatedIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }

    // publish shards
    publisShards(nodes, nodeQueues);

    // balance the index does not change anything
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    balanceOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(0, nodeqQueue.size());
    }

    // decrease the replication count and then balance again
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    indexMD.setReplicationLevel(2);
    _protocol.updateIndexMD(indexMD);
    balanceOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
  }

  @Test
  public void testUnbalancedIndexAfterBalancingIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();

    // index deployed on 2 nodes / desired replica is 3
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    publisShards(nodes, nodeQueues);

    // balance the index does not change anything
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    balanceOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(0, nodeqQueue.size());
    }

    // node completion does not add another balance op since not enough nodes
    // are there
    MasterQueue masterQueue = _protocol.publishMaster(Mocks.mockMaster());
    assertEquals(0, masterQueue.size());
    balanceOperation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertEquals(0, masterQueue.size());

    // add node and now the balance op should add itself for retry
    Node node3 = Mocks.mockNode();
    NodeQueue nodeQueue3 = Mocks.publisNode(_protocol, node3);
    balanceOperation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertEquals(1, masterQueue.size());

    // now do the balance
    assertEquals(0, nodeQueue3.size());
    balanceOperation.execute(_context, EMPTY_LIST);
    assertEquals(1, nodeQueue3.size());
    publisShard(node3, nodeQueue3);

    // now it shouldn't add itself again since the index is balanced
    balanceOperation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertEquals(1, masterQueue.size());
  }

  @Test
  public void testBalanceErrorIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();
    assertTrue(_protocol.getIndexMD(_indexName).hasDeployError());
    assertNotNull(_protocol.getIndexMD(_indexName).getDeployError());

    // balance the index should remove the error
    publisShards(nodes, nodeQueues);
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    balanceOperation.execute(_context, EMPTY_LIST);
    balanceOperation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertNull(_protocol.getIndexMD(_indexName).getDeployError());
  }

  @Test
  public void testStopBalance_WhenSourceFileDoesNotExistAnymore() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();

    // index deployed on 2 nodes / desired replica is 3
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    publisShards(nodes, nodeQueues);

    // add node and then balance again
    Node node3 = Mocks.mockNode();
    NodeQueue nodeQueue3 = Mocks.publisNode(_protocol, node3);
    assertEquals(0, nodeQueue3.size());
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    FileSystem fileSystem = mock(FileSystem.class);
    when(fileSystem.exists(any(Path.class))).thenReturn(false);
    MasterContext spiedContext = spy(_context);
    doReturn(fileSystem).when(spiedContext).getFileSystem(any(IndexMetaData.class));
    List<OperationId> nodeOperations = balanceOperation.execute(spiedContext, EMPTY_LIST);
    assertEquals(null, nodeOperations);
  }

  @Test
  public void testStopBalance_CantAccessSourceFile() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();

    // index deployed on 2 nodes / desired replica is 3
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    publisShards(nodes, nodeQueues);

    // add node and then balance again
    Node node3 = Mocks.mockNode();
    NodeQueue nodeQueue3 = Mocks.publisNode(_protocol, node3);
    assertEquals(0, nodeQueue3.size());
    BalanceIndexOperation balanceOperation = new BalanceIndexOperation(_indexName);
    MasterContext spiedContext = spy(_context);
    doThrow(new RuntimeException("test-exception")).when(spiedContext).getFileSystem(any(IndexMetaData.class));
    List<OperationId> nodeOperations = balanceOperation.execute(spiedContext, EMPTY_LIST);
    assertEquals(null, nodeOperations);
  }

}
