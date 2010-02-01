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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.operation.node.DeployResult;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.operation.node.ShardDeployOperation;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexDeployError.ErrorType;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;

public class IndexDeployOperationTest extends AbstractMasterNodeZkTest {

  @Test
  public void testDeployError_NoNodes() throws Exception {
    IndexDeployOperation deployCommand = new IndexDeployOperation(_indexName, _indexPath, 3);
    deployCommand.execute(_context, EMPTY_LIST);
    checkDeployError(ErrorType.NO_NODES_AVAILIBLE, _shardCount);
  }

  @Test
  public void testDeployError_IndexNotAccessable() throws Exception {
    IndexDeployOperation deployCommand = new IndexDeployOperation(_indexName, "wrongIndexPath", 3);
    deployCommand.execute(_context, EMPTY_LIST);
    checkDeployError(ErrorType.INDEX_NOT_ACCESSIBLE, 0);
  }

  private void checkDeployError(ErrorType errorType, int shardCount) throws Exception {
    // check results
    assertEquals(1, _protocol.getIndices().size());
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertNotNull(indexMD);
    assertTrue(indexMD.hasDeployError());
    IndexDeployError error = indexMD.getDeployError();
    assertNotNull(error);
    assertEquals(errorType, error.getErrorType());
    Set<Shard> shards = indexMD.getShards();
    assertEquals(shardCount, shards.size());
    for (Shard shard : shards) {
      assertTrue(_protocol.getShardNodes(shard.getName()).isEmpty());
    }
  }

  @Test
  public void testDeployError_ShardsNotDeployable() throws Exception {
    // add nodes
    List<Node> nodes = Mocks.mockNodes(3);
    Mocks.publisNodes(_protocol, nodes);

    // add index
    int replicationLevel = 3;
    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    operation.execute(_context, EMPTY_LIST);

    // now complete the deployment
    operation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    checkDeployError(ErrorType.SHARDS_NOT_DEPLOYABLE, _shardCount);
  }

  @Test
  public void testDeployErrorExceptions_ShardsNotDeployable() throws Exception {
    // add nodes
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);

    // add index
    int replicationLevel = 3;
    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    operation.execute(_context, EMPTY_LIST);

    // now complete the deployment
    List<OperationResult> results = new ArrayList<OperationResult>();
    for (NodeQueue nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.peek();
      DeployResult deployResult = new DeployResult(_indexName);
      Set<String> nodeShards = ((ShardDeployOperation) nodeOperation).getShardNames();
      for (String shardName : nodeShards) {
        deployResult.addShardException(shardName, new Exception());
      }
      results.add(deployResult);
    }

    operation.nodeOperationsComplete(_context, results);
    checkDeployError(ErrorType.SHARDS_NOT_DEPLOYABLE, _shardCount);

    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    IndexDeployError error = indexMD.getDeployError();
    Set<Shard> shards = indexMD.getShards();
    for (Shard shard : shards) {
      assertEquals(3, error.getShardErrors(shard.getName()).size());
    }
  }

  @Test
  public void testDeploy() throws Exception {
    // add nodes
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);

    // add index
    int replicationLevel = 3;
    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    operation.execute(_context, EMPTY_LIST);

    // check results
    Set<String> shards = new HashSet<String>();
    int shardOnNodeCount = 0;
    for (NodeQueue nodeQueue : nodeQueues) {
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

    // now complete the deployment
    publisShards(nodes, nodeQueues);
    operation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertEquals(1, _protocol.getIndices().size());
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertNotNull(indexMD);
    assertNull(indexMD.getDeployError());
  }

  @Test
  public void testDeployRespectsCurrentRunningDeployments() throws Exception {
    // add nodes
    List<Node> nodes = Mocks.mockNodes(2 * _shardCount);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);

    // add index
    int replicationLevel = 1;
    IndexDeployOperation operation1 = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    operation1.execute(_context, EMPTY_LIST);
    List<MasterOperation> runningOps = new ArrayList<MasterOperation>();
    runningOps.add(operation1);
    IndexDeployOperation operation2 = new IndexDeployOperation(_indexName + "2", _indexPath, replicationLevel);
    operation2.execute(_context, runningOps);

    // check results
    List<Integer> nodeQueueSizes = new ArrayList<Integer>();
    for (NodeQueue nodeQueue : nodeQueues) {
      nodeQueueSizes.add(nodeQueue.size());
    }
    for (Integer integer : nodeQueueSizes) {
      assertEquals("unequal shard distribution: " + nodeQueueSizes, 1, integer.intValue());
    }
  }

  @Test
  public void testDeployShardMd() throws Exception {
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> queues = Mocks.publisNodes(_protocol, nodes);

    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, 3);
    operation.execute(_context, EMPTY_LIST);
    publisShards(nodes, queues);

    ArrayList<OperationResult> results = new ArrayList<OperationResult>();
    DeployResult deployResult1 = new DeployResult(nodes.get(0).getName());
    DeployResult deployResult2 = new DeployResult(nodes.get(1).getName());
    DeployResult deployResult3 = new DeployResult(nodes.get(2).getName());
    Map<String, String> metaMap = new HashMap<String, String>();
    metaMap.put("a", "1");
    String shard1Name = AbstractIndexOperation.createShardName(_indexName, _indexFile.listFiles()[0].getAbsolutePath());
    deployResult1.addShardMetaDataMap(shard1Name, metaMap);
    deployResult2.addShardMetaDataMap(shard1Name, metaMap);
    deployResult3.addShardMetaDataMap(shard1Name, metaMap);
    results.add(deployResult1);
    results.add(deployResult2);
    results.add(deployResult3);

    operation.nodeOperationsComplete(_context, results);
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertEquals(1, indexMD.getShard(shard1Name).getMetaDataMap().size());
    assertEquals(metaMap, indexMD.getShard(shard1Name).getMetaDataMap());
  }

  @Test
  public void testDeployShardMdWithMissingNodeResult() throws Exception {
    List<Node> nodes = Mocks.mockNodes(3);
    Mocks.publisNodes(_protocol, nodes);
    List<NodeQueue> queues = Mocks.publisNodes(_protocol, nodes);

    IndexDeployOperation operation = new IndexDeployOperation(_indexName, _indexPath, 3);
    operation.execute(_context, EMPTY_LIST);
    publisShards(nodes, queues);

    ArrayList<OperationResult> results = new ArrayList<OperationResult>();
    DeployResult deployResult1 = new DeployResult(nodes.get(0).getName());
    DeployResult deployResult2 = null;
    DeployResult deployResult3 = new DeployResult(nodes.get(2).getName());
    Map<String, String> metaMap = new HashMap<String, String>();
    metaMap.put("a", "1");
    String shard1Name = AbstractIndexOperation.createShardName(_indexName, _indexFile.listFiles()[0].getAbsolutePath());
    deployResult1.addShardMetaDataMap(shard1Name, metaMap);
    deployResult3.addShardMetaDataMap(shard1Name, metaMap);
    results.add(deployResult1);
    results.add(deployResult2);
    results.add(deployResult3);

    operation.nodeOperationsComplete(_context, results);
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertEquals(1, indexMD.getShard(shard1Name).getMetaDataMap().size());
    assertEquals(metaMap, indexMD.getShard(shard1Name).getMetaDataMap());
  }

  @Test
  public void testDeployUnderreplicatedIndex() throws Exception {
    // add nodes
    List<Node> nodes = Mocks.mockNodes(3);
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, nodes);

    // add index
    int replicationLevel = 3;
    IndexDeployOperation deployOperation = new IndexDeployOperation(_indexName, _indexPath, replicationLevel);
    deployOperation.execute(_context, EMPTY_LIST);

    // publis only for one node
    publisShard(nodes.get(0), nodeQueues.get(0));

    deployOperation.nodeOperationsComplete(_context, Collections.EMPTY_LIST);
    assertEquals(1, _protocol.getIndices().size());
    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    assertNotNull(indexMD);
    assertNull(indexMD.getDeployError());

    // balance index should have been be triggered
    Master master = Mocks.mockMaster();
    MasterQueue masterQueue = _protocol.publishMaster(master);
    MasterOperation operation = masterQueue.peek();
    assertNotNull(operation);
    assertTrue(operation instanceof BalanceIndexOperation);
  }

}
