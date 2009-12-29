/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.endsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.OperationResult;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

public class MasterQueueTest extends AbstractZkTest {

  private String getRootPath() {
    // this path is cleaned up by ZkSystem!
    return _zk.getZkConf().getZkRootPath() + "/queue";
  }

  @Test(timeout = 15000)
  public void testWatchDogMechanism() throws Exception {
    final MasterQueue queue = new MasterQueue(_zk.getZkClient(), getRootPath());
    MasterOperation masterOperation = mock(MasterOperation.class);
    queue.add(masterOperation);

    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(new OperationId("node1", "e1"));

    queue.moveOperationToWatching(masterOperation, operationIds);
    assertTrue(queue.isEmpty());
    assertEquals(1, queue.getWatchdogs().size());
  }

  @Test(timeout = 15000)
  public void testKeepStateBetweenExecutionAndCompletion() throws Exception {
    final MasterQueue queue = new MasterQueue(_zk.getZkClient(), getRootPath());

    MasterOperation masterOperation = new StatefulMasterOperation();
    queue.add(masterOperation);

    masterOperation = queue.peek();
    masterOperation.execute(mock(MasterContext.class), Collections.EMPTY_LIST);

    queue.moveOperationToWatching(masterOperation, Collections.EMPTY_LIST);
    assertEquals(1, queue.getWatchdogs().size());
    masterOperation = queue.getWatchdogs().get(0).getOperation();
    masterOperation.nodeOperationsComplete(mock(MasterContext.class), Collections.EMPTY_LIST);
  }

  @Test(timeout = 15000)
  public void testWatchDogCleanup() throws Exception {
    ZkClient zkClientSpy = spy(_zk.getZkClient());
    MasterQueue queue = new MasterQueue(zkClientSpy, getRootPath());
    MasterOperation masterOperation = mock(MasterOperation.class);
    String elementName = queue.add(masterOperation);

    List<OperationId> operationIds = new ArrayList<OperationId>();
    operationIds.add(new OperationId("node1", "e1"));

    // cause a unclean state
    doThrow(new IllegalStateException("test exception")).when(zkClientSpy).delete(endsWith(elementName));
    try {
      queue.moveOperationToWatching(masterOperation, operationIds);
      verify(zkClientSpy).delete(endsWith(elementName));
      fail("should throw exception");
    } catch (Exception e) {
      // expected
    }

    // we have now both, a operation and the operation for it
    assertEquals(1, queue.getWatchdogs().size());
    assertFalse(queue.isEmpty());

    // this should only be possible if zk connection fails so we try to cleanup
    // on queue initialization
    queue = new MasterQueue(_zk.getZkClient(), getRootPath());
    assertEquals(1, queue.getWatchdogs().size());
    assertTrue(queue.isEmpty());
  }

  static class StatefulMasterOperation implements MasterOperation {
    private static final long serialVersionUID = 1L;
    private boolean _executed = false;

    @Override
    public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
      _executed = true;
      return null;
    }

    @Override
    public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
      return ExecutionInstruction.EXECUTE;
    }

    @Override
    public void nodeOperationsComplete(MasterContext context, List<OperationResult> results) throws Exception {
      assertTrue(_executed);
    }
  }

}
