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
package net.sf.katta.protocol.operation.leader;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.DistributedBlockingQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.ShardDeployOperation;

import org.junit.Test;

public class BalanceIndexOperationTest extends AbstractLeaderTest {

  @Test
  public void testBalanceUnderreplicatedIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = mockNodes(2);
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    IndexDeployOperation deployOperation = new IndexDeployOperation(_indexName, _indexPath, 3);
    deployOperation.execute(_context);

    // index deployed on 2 nodes / desired replica is 3
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    publisShards(nodes, nodeQueues);

    // balance the index does not change anything
    IndexBalanceOperation balanceOperation = new IndexBalanceOperation(_indexName);
    balanceOperation.execute(_context);
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }

    // add node and then balance again
    Node node3 = mockNode();
    DistributedBlockingQueue<NodeOperation> nodeQueue3 = publisNode(node3);
    assertEquals(0, nodeQueue3.size());

    balanceOperation.execute(_context);
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }
    assertEquals(1, nodeQueue3.size());
    _zk.showStructure();
  }

  @Test
  public void testBalanceOverreplicatedIndex() throws Exception {
    // add nodes and index
    List<Node> nodes = mockNodes(3);
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(nodes);
    IndexDeployOperation deployOperation = new IndexDeployOperation(_indexName, _indexPath, 3);
    deployOperation.execute(_context);
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }

    // publish shards
    publisShards(nodes, nodeQueues);

    // balance the index does not change anything
    IndexBalanceOperation balanceOperation = new IndexBalanceOperation(_indexName);
    balanceOperation.execute(_context);
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(1, nodeqQueue.size());
    }

    // decrease the replication count and then balance again
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    indexMD.setReplicationLevel(2);
    _protocol.updateIndexMD(indexMD);
    balanceOperation.execute(_context);
    for (DistributedBlockingQueue<NodeOperation> nodeqQueue : nodeQueues) {
      assertEquals(2, nodeqQueue.size());
    }
    _zk.showStructure();
  }

  private void publisShards(List<Node> nodes, List<DistributedBlockingQueue<NodeOperation>> nodeQueues)
          throws InterruptedException {
    for (int i = 0; i < nodes.size(); i++) {
      Set<String> shardNames = ((ShardDeployOperation) nodeQueues.get(i).peek()).getShardNames();
      for (String shardName : shardNames) {
        _protocol.publishShard(nodes.get(i), shardName, new HashMap<String, String>());
      }
    }
  }

}
