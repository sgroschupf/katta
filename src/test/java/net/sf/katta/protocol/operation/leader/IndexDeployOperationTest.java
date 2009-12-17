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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.DistributedBlockingQueue;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.ShardDeployOperation;

import org.junit.Test;

public class IndexDeployOperationTest extends AbstractLeaderTest {

  @Test
  public void testDeployWithNoNodes() throws Exception {
    IndexDeployOperation deployCommand = new IndexDeployOperation(_indexName, _indexPath, 3);
    deployCommand.execute(_context);

    // check results
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertNotNull(indexMD);
    assertEquals(IndexState.ERROR, indexMD.getState());// no nodes

    // TODO should be in error indices ?
    assertEquals(1, _protocol.getLiveIndices().size());
    Set<Shard> shards = indexMD.getShards();
    assertEquals(_shardCount, shards.size());

    Map<String, List<String>> shard2NodesMap = _protocol.getShard2NodesMap(shards);
    // TODO Map<String, List<ShardError>> shard2ErrorMap =
    // _protocol.getShard2ErrorMap(shards);
    for (Shard shard : shards) {
      assertTrue(_protocol.getShardNodes(shard.getName()).isEmpty());
      // assertTrue(shard2ErrorMap.get(shard).isEmpty());
    }
    _zk.showStructure();
  }

  @Test
  public void testDeployWithNodes() throws Exception {
    // add nodes
    List<Node> nodes = mockNodes(3);
    List<DistributedBlockingQueue<NodeOperation>> nodeQueues = publisNodes(nodes);

    // add index
    int replicationLevel = 3;
    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    operation.execute(_context);

    // check results
    Set<String> shards = new HashSet<String>();
    int shardOnNodeCount = 0;
    for (DistributedBlockingQueue<NodeOperation> nodeQueue : nodeQueues) {
      assertEquals(1, nodeQueue.size());
      NodeOperation nodeOperation = nodeQueue.peek();
      assertNotNull(nodeOperation);
      assertTrue(nodeOperation instanceof ShardDeployOperation);

      Set<String> nodeShards = ((ShardDeployOperation) nodeOperation).getShardNames();
      assertEquals(_shardCount, nodeShards.size());
      shards.addAll(nodeShards);
      shardOnNodeCount += nodeShards.size();
    }
    assertEquals(_shardCount * replicationLevel, shardOnNodeCount);
    assertEquals(_shardCount, shards.size());
    _zk.showStructure();
  }

}
