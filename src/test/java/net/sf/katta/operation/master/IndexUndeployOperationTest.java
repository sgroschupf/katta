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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;

public class IndexUndeployOperationTest extends AbstractMasterNodeZkTest {

  @Test
  public void testUndeployIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<OperationQueue<NodeOperation>> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndex(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNull(_protocol.getIndexMD(_indexName).getDeployError());

    // balance the index does not change anything
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context, EMPTY_LIST);
    for (OperationQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertEquals(0, _protocol.getIndices().size());
  }

  @Test
  public void testUndeployErrorIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<OperationQueue<NodeOperation>> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();
    publisShards(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNotNull(_protocol.getIndexMD(_indexName).getDeployError());

    // balance the index does not change anything
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context, EMPTY_LIST);
    for (OperationQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertEquals(0, _protocol.getIndices().size());
  }

}
