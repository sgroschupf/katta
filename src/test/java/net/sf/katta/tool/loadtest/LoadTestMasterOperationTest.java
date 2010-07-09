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
package net.sf.katta.tool.loadtest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.katta.AbstractTest;
import net.sf.katta.master.DefaultDistributionPolicy;
import net.sf.katta.master.Master;
import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.testutil.Mocks;
import net.sf.katta.testutil.TestIoUtil;
import net.sf.katta.tool.loadtest.query.AbstractQueryExecutor;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class LoadTestMasterOperationTest extends AbstractTest {

  private MasterQueue _queue = mock(MasterQueue.class);
  private InteractionProtocol _protocol = mock(InteractionProtocol.class);
  private Master _master = Mocks.mockMaster();
  private MasterContext _context = new MasterContext(_protocol, _master, new DefaultDistributionPolicy(), _queue);
  private AbstractQueryExecutor _queryExecutor = mock(AbstractQueryExecutor.class);

  @Test
  public void testNodeCount() throws Exception {
    int nodeCount = 2;
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    LoadTestMasterOperation operation = new LoadTestMasterOperation(nodeCount, 10, 20, 2, 20000, _queryExecutor,
            _temporaryFolder.newFolder("result"));
    operation.execute(_context, Collections.EMPTY_LIST);
    verify(_protocol, times(nodeCount)).addNodeOperation(anyString(), any(NodeOperation.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testMasterChange() throws Exception {
    int nodeCount = 2;
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    LoadTestMasterOperation operation = new LoadTestMasterOperation(nodeCount, 10, 20, 2, 20000, _queryExecutor,
            _temporaryFolder.newFolder("result"));
    operation.execute(_context, Collections.EMPTY_LIST);

    _master = Mocks.mockMaster();
    _context = new MasterContext(_protocol, _master, new DefaultDistributionPolicy(), _queue);
    operation.execute(_context, Collections.EMPTY_LIST);
  }

  @Test
  public void testNodeOperation() throws Exception {
    int nodeCount = 2;
    int startRate = 10;
    int endRate = 30;
    int step = 10;
    int runTime = 20000;

    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    LoadTestMasterOperation operation = new LoadTestMasterOperation(nodeCount, startRate, endRate, step, runTime,
            _queryExecutor, _temporaryFolder.newFolder("result"));

    // iteration I
    operation.execute(_context, Collections.EMPTY_LIST);
    checkNodeOperation(0, nodeCount, startRate, step, runTime);

    // iteration II
    reset(_protocol, _queue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    operationResults.add(new LoadTestNodeOperationResult("n1", Arrays.asList(mock(LoadTestQueryResult.class))));
    operationResults.add(new LoadTestNodeOperationResult("n2", Arrays.asList(mock(LoadTestQueryResult.class))));
    operation.nodeOperationsComplete(_context, operationResults);
    ArgumentCaptor<MasterOperation> argumentCaptor2 = ArgumentCaptor.forClass(MasterOperation.class);
    verify(_queue).add(argumentCaptor2.capture());

    operation = (LoadTestMasterOperation) argumentCaptor2.getValue();
    operation.execute(_context, Collections.EMPTY_LIST);
    checkNodeOperation(1, nodeCount, startRate, step, runTime);

    // iteration III
    reset(_protocol, _queue);
    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    operation.nodeOperationsComplete(_context, operationResults);
    argumentCaptor2 = ArgumentCaptor.forClass(MasterOperation.class);
    verify(_queue).add(argumentCaptor2.capture());
    operation = (LoadTestMasterOperation) argumentCaptor2.getValue();

    operation = (LoadTestMasterOperation) argumentCaptor2.getValue();
    operation.execute(_context, Collections.EMPTY_LIST);
    checkNodeOperation(2, nodeCount, startRate, step, runTime);

    // finsih
    reset(_protocol, _queue);
    operation.nodeOperationsComplete(_context, operationResults);
    verify(_queue, times(0)).add((MasterOperation) any());
  }

  private void checkNodeOperation(int iteration, int nodeCount, int startRate, int step, int runTime) {
    ArgumentCaptor<NodeOperation> argumentCaptor1 = ArgumentCaptor.forClass(NodeOperation.class);
    verify(_protocol, times(nodeCount)).addNodeOperation(anyString(), argumentCaptor1.capture());
    LoadTestNodeOperation nodeOperation = (LoadTestNodeOperation) argumentCaptor1.getValue();
    assertEquals((startRate + iteration * step) / nodeCount, nodeOperation._queryRate);
    assertEquals(runTime, nodeOperation._runTime);
  }

  @Test
  public void testResultFiles() throws Exception {
    int nodeCount = 2;
    int startRate = 10;
    int endRate = 20;
    int step = 10;
    int runTime = 20000;

    when(_protocol.getLiveNodes()).thenReturn(Arrays.asList("n1", "n2", "n3"));
    File resultFolder = _temporaryFolder.newFolder("result");
    assertEquals(0, resultFolder.list().length);
    LoadTestMasterOperation operation = new LoadTestMasterOperation(nodeCount, startRate, endRate, step, runTime,
            _queryExecutor, resultFolder);

    // iteration I
    operation.execute(_context, Collections.EMPTY_LIST);
    operation = (LoadTestMasterOperation) serializeDesirialize(operation);

    List<OperationResult> operationResults = new ArrayList<OperationResult>();
    operationResults.add(new LoadTestNodeOperationResult("n1", Arrays.asList(mock(LoadTestQueryResult.class))));
    operationResults.add(new LoadTestNodeOperationResult("n2", Arrays.asList(mock(LoadTestQueryResult.class))));
    operation.nodeOperationsComplete(_context, operationResults);
    assertEquals(2, resultFolder.list().length);
    ArgumentCaptor<MasterOperation> argumentCaptor2 = ArgumentCaptor.forClass(MasterOperation.class);
    verify(_queue).add(argumentCaptor2.capture());

    // iteration II
    operation = (LoadTestMasterOperation) argumentCaptor2.getValue();
    operation = (LoadTestMasterOperation) serializeDesirialize(operation);
    operation.execute(_context, Collections.EMPTY_LIST);
    operation = (LoadTestMasterOperation) serializeDesirialize(operation);
    operation.nodeOperationsComplete(_context, operationResults);

    assertEquals(2, resultFolder.list().length);

    File[] listFiles = resultFolder.listFiles();
    File logFile;
    File resultFile;
    if (listFiles[0].getName().contains("-results-")) {
      resultFile = listFiles[0];
      logFile = listFiles[1];
    } else {
      resultFile = listFiles[1];
      logFile = listFiles[0];
    }
    assertEquals(1 + 4, TestIoUtil.countLines(logFile));
    assertEquals(1 + 2, TestIoUtil.countLines(resultFile));
  }

  private Serializable serializeDesirialize(Serializable serializable) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
    ObjectOutputStream stream = new ObjectOutputStream(byteArrayOS);
    stream.writeObject(serializable);
    stream.close();

    ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(byteArrayOS.toByteArray()));
    try {
      return (Serializable) inputStream.readObject();
    } finally {
      inputStream.close();
    }
  }
}
