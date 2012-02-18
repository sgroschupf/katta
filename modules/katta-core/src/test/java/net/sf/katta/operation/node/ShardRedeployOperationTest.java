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
package net.sf.katta.operation.node;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class ShardRedeployOperationTest extends AbstractNodeOperationMockTest {

  @Before
  public void setUp() {
    when(_shardManager.getShardFolder((String) notNull())).thenReturn(new File("shardFolder"));
  }

  @Test
  public void testRedeploy() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");
    ShardRedeployOperation operation = new ShardRedeployOperation(shards);
    operation.execute(_context);

    InOrder inOrder = inOrder(_protocol, _contentServer);
    for (String shard : shards) {
      inOrder.verify(_contentServer).addShard(eq(shard), (File) notNull());
      inOrder.verify(_protocol).publishShard(eq(_node), eq(shard));
    }
  }

  @Test
  public void testRedeployShardAlreadyKnownToNodeManaged() throws Exception {
    List<String> shards = Arrays.asList("shard1", "shard2");

    when(_contentServer.getShards()).thenReturn(shards);
    ShardRedeployOperation operation = new ShardRedeployOperation(shards);
    operation.execute(_context);

    // only publis but not add to nodemanaged again
    InOrder inOrder = inOrder(_protocol, _contentServer);
    for (String shard : shards) {
      inOrder.verify(_protocol).publishShard(eq(_node), eq(shard));
    }
  }
}
