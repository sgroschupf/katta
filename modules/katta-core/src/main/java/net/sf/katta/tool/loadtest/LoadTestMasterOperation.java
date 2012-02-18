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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.master.MasterContext;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.tool.loadtest.query.AbstractQueryExecutor;

import org.apache.commons.math.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class LoadTestMasterOperation implements MasterOperation {

  private static final Logger LOG = Logger.getLogger(LoadTestMasterOperation.class);

  private String _masterName;
  private final int _numberOfTesterNodes;
  private final int _startRate;
  private final int _endRate;
  private final int _step;

  private final long _runTime;
  private final AbstractQueryExecutor _queryExecutor;
  private final File _resultDir;
  private final long _startTime;
  private int _currentIteration;
  private long _currentIterationStartTime;

  public LoadTestMasterOperation(int nodes, int startRate, int endRate, int step, long runTime,
          AbstractQueryExecutor queryExecutor, File resultDir) {
    _numberOfTesterNodes = nodes;
    _startRate = startRate;
    _endRate = endRate;
    _step = step;
    _queryExecutor = queryExecutor;
    _runTime = runTime;
    _resultDir = resultDir;

    _startTime = System.currentTimeMillis();
  }

  @Override
  public ExecutionInstruction getExecutionInstruction(List<MasterOperation> runningOperations) throws Exception {
    return ExecutionInstruction.EXECUTE;
  }

  private String getName() {
    return "load-test-" + _startTime;
  }

  public void registerCompletion(InteractionProtocol protocol) {
    protocol.setFlag(getName());
  }

  public void joinCompletion(InteractionProtocol protocol) throws InterruptedException {
    while (protocol.flagExists(getName())) {
      Thread.sleep(1000);
    }
  }

  @Override
  public List<OperationId> execute(MasterContext context, List<MasterOperation> runningOperations) throws Exception {
    _currentIterationStartTime = System.currentTimeMillis();
    if (_masterName == null) {
      _masterName = context.getMaster().getMasterName();
      _resultDir.mkdirs();
      if (!_resultDir.isDirectory()) {
        throw new IllegalStateException("result dir '" + _resultDir.getAbsolutePath() + "' cannot be created");
      }
    } else if (!_masterName.equals(context.getMaster().getMasterName())) {
      throw new IllegalStateException(
              "master change detected - load test not safe for this since it writes local log files!");
    }
    List<String> testNodes = context.getProtocol().getLiveNodes();
    if (testNodes.size() < _numberOfTesterNodes) {
      throw new IllegalStateException("only " + testNodes.size() + " available, needing " + _numberOfTesterNodes);
    }
    testNodes = testNodes.subList(0, _numberOfTesterNodes);

    final int queryRate = calculateCurrentQueryRate();
    if (_currentIteration == 0) {
      LOG.info("starting load test with " + testNodes.size() + " nodes");
    }
    LOG.info("executing tests in iteration " + _currentIteration + " at query rate: " + queryRate
            + " queries per second and with a run time of " + _runTime / 1000 + " seconds");

    int remainingQueryRate = queryRate;
    int remainingNodes = testNodes.size();

    List<OperationId> nodeOperationIds = new ArrayList<OperationId>();
    for (String testNode : testNodes) {
      int queryRateForNode = remainingQueryRate / remainingNodes;
      LOG.info("instructing test on node " + testNode + " using query rate: " + queryRateForNode
              + " queries per second.");

      LoadTestNodeOperation nodeOperation = new LoadTestNodeOperation(_queryExecutor, queryRateForNode, _runTime);
      --remainingNodes;
      remainingQueryRate -= queryRateForNode;
      OperationId operationId = context.getProtocol().addNodeOperation(testNode, nodeOperation);
      nodeOperationIds.add(operationId);
    }
    return nodeOperationIds;
  }

  private int calculateCurrentQueryRate() {
    return _startRate + _currentIteration * _step;
  }

  @Override
  public void nodeOperationsComplete(MasterContext context, List<OperationResult> nodeResults) throws Exception {
    try {
      final int queryRate = calculateCurrentQueryRate();
      LOG.info("collecting results for iteration " + _currentIteration + " and query rate " + queryRate + " after "
              + (System.currentTimeMillis() - _currentIterationStartTime) + " ms ...");
      List<LoadTestQueryResult> queryResults = new ArrayList<LoadTestQueryResult>();
      for (OperationResult operationResult : nodeResults) {
        if (operationResult == null || operationResult.getUnhandledException() != null) {
          Exception rootException = null;
          if (operationResult != null) {
            rootException = operationResult.getUnhandledException();
          }
          throw new IllegalStateException("at least one node operation did not completed properly: " + nodeResults,
                  rootException);
        }
        LoadTestNodeOperationResult nodeOperationResult = (LoadTestNodeOperationResult) operationResult;
        queryResults.addAll(nodeOperationResult.getQueryResults());
      }
      LOG.info("Received " + queryResults.size() + " queries, expected " + queryRate * _runTime / 1000);

      File statisticsFile = new File(_resultDir, "load-test-log-" + _startTime + ".log");
      File resultsFile = new File(_resultDir, "load-test-results-" + _startTime + ".log");
      Writer statisticsWriter = new OutputStreamWriter(new FileOutputStream(statisticsFile, true));
      Writer resultWriter = new OutputStreamWriter(new FileOutputStream(resultsFile, true));
      if (_currentIteration == 0) {
        // print headers
        statisticsWriter.append("#queryRate \tnode \tstartTime \tendTime \telapseTime \tquery \n");
        resultWriter
                .append("#requestedQueryRate \tachievedQueryRate \tfiredQueries \tqueryErrors \tavarageQueryDuration \tstandardDeviation  \n");
      }
      try {
        StorelessUnivariateStatistic timeStandardDeviation = new StandardDeviation();
        StorelessUnivariateStatistic timeMean = new Mean();
        int errors = 0;

        for (LoadTestQueryResult result : queryResults) {
          long elapsedTime = result.getEndTime() > 0 ? result.getEndTime() - result.getStartTime() : -1;
          statisticsWriter.write(queryRate + "\t" + result.getNodeId() + "\t" + result.getStartTime() + "\t"
                  + result.getEndTime() + "\t" + elapsedTime + "\t" + result.getQuery() + "\n");
          if (elapsedTime != -1) {
            timeStandardDeviation.increment(elapsedTime);
            timeMean.increment(elapsedTime);
          } else {
            ++errors;
          }
        }
        resultWriter.write(queryRate + "\t" + ((double) queryResults.size() / (_runTime / 1000)) + "\t"
                + queryResults.size() + "\t" + errors + "\t" + (int) timeMean.getResult() + "\t"
                + (int) timeStandardDeviation.getResult() + "\n");
      } catch (IOException e) {
        throw new IllegalStateException("Failed to write statistics data.", e);
      }
      try {
        LOG.info("results written to " + resultsFile.getAbsolutePath());
        LOG.info("statistics written to " + statisticsFile.getAbsolutePath());
        statisticsWriter.close();
        resultWriter.close();
      } catch (IOException e) {
        LOG.warn("Failed to close statistics file.");
      }
      if (queryRate + _step <= _endRate) {
        _currentIteration++;
        LOG.info("triggering next iteration " + _currentIteration);
        context.getMasterQueue().add(this);
      } else {
        LOG.info("finish load test in iteration " + _currentIteration + " after "
                + (System.currentTimeMillis() - _startTime) + " ms");
        context.getProtocol().removeFlag(getName());
      }
    } catch (Exception e) {
      context.getProtocol().removeFlag(getName());
    }
  }
}
