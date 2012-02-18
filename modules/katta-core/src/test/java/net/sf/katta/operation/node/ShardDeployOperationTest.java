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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.InOrder;

public class ShardDeployOperationTest extends AbstractNodeOperationMockTest {

  @Test
  public void testDeploy() throws Exception {
    ShardDeployOperation operation = new ShardDeployOperation();
    operation.addShard("shard1", "shardPath1");
    operation.addShard("shard2", "shardPath2");

    File shardFolder = new File("shardFolder");
    Map<String, String> shardMD = new HashMap<String, String>();
    when(_shardManager.installShard((String) notNull(), (String) notNull())).thenReturn(shardFolder);
    when(_contentServer.getShardMetaData((String) notNull())).thenReturn(shardMD);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _contentServer);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_shardManager).installShard(shard, operation.getShardPath(shard));
      inOrder.verify(_contentServer).addShard(shard, shardFolder);
      inOrder.verify(_protocol).publishShard(_node, shard);
    }
    assertEquals(0, result.getShardExceptions().size());
    assertEquals(2, result.getShardMetaDataMaps().size());
    System.out.println(result.getShardMetaDataMaps());
    for (String shardName : operation.getShardNames()) {
      assertTrue(result.getShardMetaDataMaps().containsKey(shardName));
    }
  }

  @Test
  public void testDeployWithOneFailingShard() throws Exception {
    ShardDeployOperation operation = new ShardDeployOperation();
    operation.addShard("shard1", "shardPath1");
    operation.addShard("shard2", "shardPath2");

    File shardFolder = new File("shardFolder");
    Map<String, String> shardMD = new HashMap<String, String>();
    when(_shardManager.installShard((String) notNull(), (String) notNull())).thenReturn(shardFolder);
    when(_contentServer.getShardMetaData((String) notNull())).thenReturn(shardMD);

    String failingShard = operation.getShardNames().iterator().next();
    doThrow(new Exception("testException")).when(_contentServer).addShard(failingShard, shardFolder);

    DeployResult result = operation.execute(_context);
    InOrder inOrder = inOrder(_protocol, _shardManager, _contentServer);
    for (String shard : operation.getShardNames()) {
      inOrder.verify(_shardManager).installShard(shard, operation.getShardPath(shard));
      inOrder.verify(_contentServer).addShard(shard, shardFolder);
      if (!shard.equals(failingShard)) {
        inOrder.verify(_protocol).publishShard(_node, shard);
      }
    }
    assertEquals(1, result.getShardMetaDataMaps().size());
    assertEquals(1, result.getShardExceptions().size());
    assertEquals(failingShard, result.getShardExceptions().entrySet().iterator().next().getKey());
    verify(_shardManager).uninstallShard(eq(failingShard));
  }
}
