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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.tool.SampleIndexGenerator;
import net.sf.katta.util.FileUtil;

import org.junit.Test;

public class DeployPolicyIntegrationTest extends AbstractIntegrationTest {

  File _indexWithOneShard;

  public DeployPolicyIntegrationTest() {
    super(2);
  }

  @Override
  protected void afterClusterStart() throws Exception {
    _indexWithOneShard = new File("build/testdir/oneShardedIndex");
    FileUtil.deleteFolder(_indexWithOneShard);
    SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
    sampleIndexGenerator.createIndex(new String[] { "a", "b", "c" }, _indexWithOneShard.getAbsolutePath(), 2, 3);
  }

  @Test
  public void testEqualDistributionWhenMoreNodesThenShards() throws Exception {
    int replicationCount = 1;
    AbstractIntegrationTest._miniCluster.deployTestIndexes(_indexWithOneShard, getNodeCount(), replicationCount);

    final InteractionProtocol protocol = AbstractIntegrationTest._miniCluster.getProtocol();
    Assert.assertEquals(getNodeCount(), protocol.getIndices().size());

    protocol.showStructure(false);
    List<Node> nodes = AbstractIntegrationTest._miniCluster.getNodes();
    for (Node node : nodes) {
      Assert.assertEquals(1, node.getContext().getContentServer().getShards().size());
    }

  }
}
