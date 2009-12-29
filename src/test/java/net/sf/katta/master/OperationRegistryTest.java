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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.AbstractMasterNodeZkTest;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.testutil.Mocks;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class OperationRegistryTest extends AbstractMasterNodeZkTest {

  private final OperationRegistry _registry = new OperationRegistry(_context);
  private final List<Node> _nodes = Mocks.mockNodes(5);
  private final MasterOperation _masterOperation = mock(MasterOperation.class);

  @Test(timeout = 10000)
  public void testAllOperationsDoneWithoutResults() throws Exception {
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, _nodes);
    OperationWatchdog operationWatchdog = beginMasterOperation(_nodes, _masterOperation);

    // execute node operations
    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    for (NodeQueue nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.peek();
      assertNotNull(nodeOperation);
      nodeQueue.remove();
      operationResults.add(null);
    }
    operationWatchdog.join();
    assertTrue(operationWatchdog.isDone());
    assertEquals(0, operationWatchdog.getOpenOperationCount());
    verify(_masterOperation, times(1)).nodeOperationsComplete(_context, operationResults);
  }

  @Test(timeout = 10000)
  public void testAllOperationsDone() throws Exception {
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, _nodes);
    OperationWatchdog operationWatchdog = beginMasterOperation(_nodes, _masterOperation);

    // execute node operations
    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    int i = 0;
    for (NodeQueue nodeQueue : nodeQueues) {
      NodeOperation nodeOperation = nodeQueue.peek();
      assertNotNull(nodeOperation);
      OperationResult result = new OperationResult(_nodes.get(i).getName());
      operationResults.add(result);
      nodeQueue.complete(result);
      i++;
    }
    operationWatchdog.join();
    assertTrue(operationWatchdog.isDone());
    assertEquals(0, operationWatchdog.getOpenOperationCount());
    ArgumentCaptor<List> argument = ArgumentCaptor.forClass(List.class);

    verify(_masterOperation, times(1)).nodeOperationsComplete(eq(_context), argument.capture());
    List<OperationResult> capturedResults = argument.getValue();
    assertEquals(operationResults.size(), capturedResults.size());
    for (OperationResult result : capturedResults) {
      assertNotNull(result);
      assertNotNull(result.getNodeName());
    }
  }

  @Test(timeout = 10000)
  public void testOneOperationsMissing() throws Exception {
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, _nodes);
    OperationWatchdog watchdog = beginMasterOperation(_nodes, _masterOperation);

    // execute operation
    nodeQueues.get(0).remove();
    nodeQueues.get(1).remove();
    nodeQueues.get(2).remove();

    // deployment be still pending
    watchdog.join(250);
    assertFalse(watchdog.isDone());
    assertEquals(2, watchdog.getOpenOperationCount());
    watchdog.cancel();
  }

  @Test(timeout = 10000)
  public void testOperationsDoneOrNodeGone() throws Exception {
    List<NodeQueue> nodeQueues = Mocks.publisNodes(_protocol, _nodes);
    OperationWatchdog watchdog = beginMasterOperation(_nodes, _masterOperation);

    nodeQueues.get(0).remove();
    nodeQueues.get(1).remove();
    nodeQueues.get(2).remove();
    _protocol.unregisterComponent(_nodes.get(3));
    _protocol.unregisterComponent(_nodes.get(4));

    // deployment should be complete
    watchdog.join();
    assertTrue(watchdog.isDone());
    assertEquals(0, watchdog.getOpenOperationCount());
  }

  @Test(timeout = 10000)
  public void testGetRunningOperations() throws Exception {
    Mocks.publisNodes(_protocol, _nodes);
    beginMasterOperation(_nodes, _masterOperation);
    assertEquals(1, _registry.getRunningOperations().size());
    _registry.shutdown();
  }

  private OperationWatchdog beginMasterOperation(List<Node> nodes, MasterOperation operation) {
    List<OperationId> operationIds = new ArrayList<OperationId>();
    for (Node node : nodes) {
      OperationId operationId = _protocol.addNodeOperation(node.getName(), mock(NodeOperation.class));
      operationIds.add(operationId);
    }
    OperationWatchdog watchdog = new OperationWatchdog("id", operation, operationIds);
    _registry.watchFor(watchdog);
    assertFalse(watchdog.isDone());
    assertEquals(nodes.size(), watchdog.getOpenOperationCount());
    return watchdog;
  }
}
