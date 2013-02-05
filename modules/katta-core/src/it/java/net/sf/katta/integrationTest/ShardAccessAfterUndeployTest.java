/**
 * Copyright 2012 the original author or authors.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.sf.katta.client.*;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.node.IContentServer;
import net.sf.katta.operation.master.IndexUndeployOperation;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class ShardAccessAfterUndeployTest extends AbstractIntegrationTest {
  public ShardAccessAfterUndeployTest() {
    super(1);
  }

  @Test
  public void testSearchAfterUndeploy() throws Exception {
    deployTestIndices(1, 1);
    final IContentServer contentServer = _miniCluster.getNode(0).getContext().getContentServer();

    // Get the shard names we will expect to be unavailable later
    final Set<String> expectedMissingShards = ImmutableSet.copyOf(contentServer.getShards());

    /* Create special distribution policy that simulates shards being undeployed during an operation:
     *  * On the first call to createNode2ShardsMap, get the createNode2ShardsMap result from the parent class, then
     *    undeploy the index and wait for the shards to become unavailable from the content server before continuing.
     *  * Subsequent calls simply return the parent class's createNode2ShardsMap result.
     * This reliably simulates the error condition (index undeployed after the client determines which nodes to
     * connect to, but before the client gets results from the nodes.)
     */
    final INodeSelectionPolicy trickNodeSelectionPolicy = new BasicNodeSelectionPolicy() {
      private boolean firstCall = true;

      @Override
      public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
        if(!firstCall) {
          return super.createNode2ShardsMap(shards);
        } else {
          firstCall = false;
          IndexUndeployOperation undeployOperation = new IndexUndeployOperation(INDEX_NAME);
          _miniCluster.getProtocol().addMasterOperation(undeployOperation);
          while(contentServer.getShards().size() > 0) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }

          return ImmutableMap.of(_miniCluster.getNode(0).getName(), (List<String>)ImmutableList.copyOf(expectedMissingShards));
        }
      }
    };

    // Attempt a search operation
    Client client = new Client(IContentServer.class, trickNodeSelectionPolicy);
    // note - getShardMetaData only operates on one shard, but Client is unaware of this
    ClientResult result = client.broadcastToIndices(10000, true, IContentServer.class.getMethod("getShardMetaData", new Class[]{String.class}), -1, null, expectedMissingShards.iterator().next());

    // Verify all shards were missing, and there were no errors.
    assertThat("Results object should be returned from Client", result, is(notNullValue()));
    assertThat("No errors were found", result.getError(), is(nullValue()));
    assertThat("All the shards should be missing", result.getMissingShards(), is(equalTo((Set)expectedMissingShards)));
  }
}

