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
package net.sf.katta.integrationTest;

import net.sf.katta.integrationTest.support.AbstractMapFileIntegrationTest;
import net.sf.katta.node.Node;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestUtil;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NodeIntegrationTest extends AbstractMapFileIntegrationTest {

  public NodeIntegrationTest() {
    super(2);
  }

  @Test
  public void testDeployShardAfterRestart() throws Exception {
    deployTestIndices(1, getNodeCount());
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    assertEquals(1, protocol.getIndices().size());

    Collection<String> deployedShards = protocol.getNodeShards(_miniCluster.getNode(0).getName());
    assertFalse(deployedShards.isEmpty());

    // restart node
    Node node = _miniCluster.restartNode(0);
    assertEquals(deployedShards, protocol.getNodeShards(node.getName()));
  }

  @Test
  public void testUndeployShard() throws Exception {
    deployTestIndices(1, getNodeCount());
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    assertEquals(1, protocol.getIndices().size());
    Node node = _miniCluster.getNode(0);
    TestUtil.waitUntilNodeServesShards(protocol, node.getName(), SHARD_COUNT);

    // we should have 4 folders in our working folder now.
    File shardsFolder = node.getContext().getShardManager().getShardsFolder();
    assertEquals(SHARD_COUNT, shardsFolder.list().length);

    ShardUndeployOperation undeployOperation = new ShardUndeployOperation(Arrays.asList(protocol.getNodeShards(
            node.getName()).iterator().next()));
    protocol.addNodeOperation(node.getName(), undeployOperation);
    TestUtil.waitUntilNodeServesShards(protocol, node.getName(), 3);
    // Thread.sleep(2000);
    assertEquals(3, shardsFolder.list().length);
  }

}
