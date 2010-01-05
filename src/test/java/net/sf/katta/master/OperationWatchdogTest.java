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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.katta.AbstractTest;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;

public class OperationWatchdogTest extends AbstractTest {

  private MasterOperation _masterOperation = mock(MasterOperation.class);
  private List<OperationId> _operationIds = Arrays.asList(new OperationId("node1", "ne1"));
  private OperationWatchdog _watchdog = new OperationWatchdog("e1", _masterOperation, _operationIds);
  private InteractionProtocol _protocol = mock(InteractionProtocol.class);
  private MasterQueue _masterQueue = mock(MasterQueue.class);
  private MasterContext _context = new MasterContext(_protocol, Mocks.mockMaster(), new DefaultDistributionPolicy(),
          _masterQueue);

  @Test
  public void testWatchdogCompletion_OperationsDone() throws Exception {
    // start watchdog - node operation pending
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList(_operationIds.get(0).getNodeName()));
    when(_protocol.isNodeOperationQueued(_operationIds.get(0))).thenReturn(true);
    _watchdog.start(_context);
    assertFalse(_watchdog.isDone());

    // finish node operation
    when(_protocol.isNodeOperationQueued(_operationIds.get(0))).thenReturn(false);
    _watchdog.checkDeploymentForCompletion();
    assertTrue(_watchdog.isDone());
    verify(_masterQueue).removeWatchdog(_watchdog);
  }

  @Test
  public void testWatchdogCompletion_NodeDown() throws Exception {
    // start watchdog - node operation pending
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList(_operationIds.get(0).getNodeName()));
    when(_protocol.isNodeOperationQueued(_operationIds.get(0))).thenReturn(true);
    _watchdog.start(_context);
    assertFalse(_watchdog.isDone());

    // node down
    when(_protocol.getLiveNodes()).thenReturn(Collections.EMPTY_LIST);
    _watchdog.checkDeploymentForCompletion();
    assertTrue(_watchdog.isDone());
    verify(_masterQueue).removeWatchdog(_watchdog);
  }
}
