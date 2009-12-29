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

import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;

public class CheckIndicesOperationTest extends AbstractMasterNodeZkTest {

  @Test
  public void testBalanceUnderreplicatedIndex() throws Exception {
    MasterQueue masterQueue = Mocks.publishMaster(_protocol);

    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndex(nodes, nodeQueues);
    assertEquals(0, masterQueue.size());

    // balance the index does not change anything
    CheckIndicesOperation checkOperation = new CheckIndicesOperation();
    checkOperation.execute(_context, EMPTY_LIST);
    assertEquals(0, masterQueue.size());

    // add node and then balance again
    Node node3 = Mocks.mockNode();
    Mocks.publisNode(_protocol, node3);
    checkOperation.execute(_context, EMPTY_LIST);
    assertEquals(1, masterQueue.size());
  }

  @Test
  public void testBalanceOverreplicatedIndex() throws Exception {
    MasterQueue masterQueue = Mocks.publishMaster(_protocol);

    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
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