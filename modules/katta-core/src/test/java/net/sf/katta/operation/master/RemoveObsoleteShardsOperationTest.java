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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.katta.master.DefaultDistributionPolicy;
import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.ShardUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class RemoveObsoleteShardsOperationTest {

  protected static final List EMPTY_LIST = Collections.EMPTY_LIST;

  @Test
  public void testMockRemove() throws Exception {
    String nodeName = "nodeA";
    String someOldShard = AbstractIndexOperation.createShardName("someOldIndex", "someOldShard");
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    MasterQueue queue = mock(MasterQueue.class);
    MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), queue);
    when(protocol.getNodeShards(nodeName)).thenReturn(Arrays.asList(someOldShard));

    RemoveObsoleteShardsOperation operation = new RemoveObsoleteShardsOperation(nodeName);
    operation.execute(context, EMPTY_LIST);

    ArgumentCaptor<NodeOperation> captor = ArgumentCaptor.forClass(NodeOperation.class);
    verify(protocol).addNodeOperation(eq(nodeName), captor.capture());
    assertThat(captor.getValue(), instanceOf(ShardUndeployOperation.class));
    ShardUndeployOperation undeployOperation = (ShardUndeployOperation) captor.getValue();
    assertEquals(1, undeployOperation.getShardNames().size());
    assertEquals(someOldShard, undeployOperation.getShardNames().iterator().next());
  }

  @Test
  public void testNotRemoveDeployingIndex() throws Exception {
    String nodeName = "nodeA";
    String indexName = "someOldIndex";
    String someOldShard = AbstractIndexOperation.createShardName(indexName, "someOldShard");
    InteractionProtocol protocol = mock(InteractionProtocol.class);
    MasterQueue queue = mock(MasterQueue.class);
    MasterContext context = new MasterContext(protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(), queue);
    when(protocol.getNodeShards(nodeName)).thenReturn(Arrays.asList(someOldShard));

    RemoveObsoleteShardsOperation operation = new RemoveObsoleteShardsOperation(nodeName);
    operation.execute(context, new ArrayList<MasterOperation>(Arrays.asList(new IndexDeployOperation(indexName, "path",
            1))));

    verify(protocol, times(0)).addNodeOperation(eq(nodeName), (NodeOperation) notNull());
  }
}
