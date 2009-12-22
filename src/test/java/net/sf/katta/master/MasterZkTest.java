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
package net.sf.katta.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.List;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.testutil.mockito.SerializableCountDownLatchAnswer;
import net.sf.katta.util.KattaException;

import org.junit.Test;

public class MasterZkTest extends AbstractZkTest {

  private static final List EMPTY_LIST = Collections.EMPTY_LIST;

  @Test
  public void testShutdown_shouldCleanupZkClientSubscriptions() throws KattaException {
    int numberOfListeners = _zk.getZkClient().numberOfListeners();
    Master master = new Master(_zk.getInteractionProtocol(), false);
    master.start();
    master.shutdown();
    assertEquals(numberOfListeners, _zk.getZkClient().numberOfListeners());
  }

  @Test(timeout = 10000)
  public void testMasterOperationPickup() throws Exception {
    Master master = new Master(_zk.getInteractionProtocol(), false);
    Node node = mock(Node.class);// leave safe mode
    _protocol.publishNode(node, new NodeMetaData("node1"));
    master.start();

    LeaderOperation operation1 = mock(LeaderOperation.class, withSettings().serializable());
    LeaderOperation operation2 = mock(LeaderOperation.class, withSettings().serializable());

    SerializableCountDownLatchAnswer answer = new SerializableCountDownLatchAnswer(2);
    when(operation1.execute((LeaderContext) notNull(), EMPTY_LIST)).thenAnswer(answer);
    when(operation2.execute((LeaderContext) notNull(), EMPTY_LIST)).thenAnswer(answer);
    _protocol.addLeaderOperation(operation1);
    _protocol.addLeaderOperation(operation2);
    answer.getCountDownLatch().await();

    master.shutdown();
  }

}
