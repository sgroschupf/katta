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

import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.testutil.Mocks;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.fest.assertions.Assertions.assertThat;

public class IndexUndeployOperationTest extends AbstractMasterNodeZkTest {

  @Test
  public void testUndeployIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndex(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNull(_protocol.getIndexMD(_indexName).getDeployError());
    assertThat(_protocol.getZkClient().getChildren(getZkConf().getZkPath(PathDef.SHARD_TO_NODES))).isNotEmpty();
    String shardName = _protocol.getIndexMD(_indexName).getShards().iterator().next().getName();

    // undeploy
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertEquals(0, _protocol.getIndices().size());
    assertThat(_protocol.getZkClient().getChildren(getZkConf().getZkPath(PathDef.SHARD_TO_NODES))).isEmpty();

    // again delete shard-to-node path - see KATTA-178
    _protocol.getZkClient().createPersistent(getZkConf().getZkPath(PathDef.SHARD_TO_NODES, shardName));
    assertThat(_protocol.getZkClient().getChildren(getZkConf().getZkPath(PathDef.SHARD_TO_NODES))).isNotEmpty();
    undeployOperation.nodeOperationsComplete(_context, EMPTY_LIST);
    assertThat(_protocol.getZkClient().getChildren(getZkConf().getZkPath(PathDef.SHARD_TO_NODES))).isEmpty();
  }

  @Test
  public void testUndeployErrorIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = Mocks.mockNodes(2);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);
    deployIndexWithError();
    publisShards(nodes, nodeQueues);
    assertNotNull(_protocol.getIndexMD(_indexName));
    assertNotNull(_protocol.getIndexMD(_indexName).getDeployError());

    // balance the index does not change anything
    IndexUndeployOperation undeployOperation = new IndexUndeployOperation(_indexName);
    undeployOperation.execute(_context, EMPTY_LIST);
    for (NodeQueue nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
      assertTrue(nodeqQueue.peek() instanceof ShardUndeployOperation);
    }
    assertNull(_protocol.getIndexMD(_indexName));
    assertEquals(0, _protocol.getIndices().size());
  }

}
