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

import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.sf.katta.node.NodeContext;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.tool.loadtest.query.AbstractQueryExecutor;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class LoadTestNodeOperation implements NodeOperation {

  protected final static Logger LOG = Logger.getLogger(LoadTestNodeOperation.class);

  protected Random _random = new Random(System.currentTimeMillis());
  protected final AbstractQueryExecutor _queryExecutor;
  protected int _queryRate;
  protected final long _runTime;

  public LoadTestNodeOperation(AbstractQueryExecutor queryExecutor, int queryRate, long runTime) {
    _queryExecutor = queryExecutor;
    _queryRate = queryRate;
    _runTime = runTime;
  }

  @Override
  public OperationResult execute(NodeContext context) throws InterruptedException {
    int threads = Math.max(1, (_queryRate - 1) / 3 + 1);
    int testDelay = 1000 * threads / _queryRate;

    LOG.info("Requested to run test at " + _queryRate + " queries per second using " + threads
            + " threads and a test delay of " + testDelay + "ms.");

    try {
      _queryExecutor.init(context);
    } catch (Exception e) {
      throw new RuntimeException("failed to init query executor " + _queryExecutor, e);
    }
    long startTime = System.currentTimeMillis();
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(threads);
    List<LoadTestQueryResult> statistics = new Vector<LoadTestQueryResult>();
    for (int i = 0; i < threads; i++) {
      TestSearcherRunnable runnable = new TestSearcherRunnable(testDelay, context, executorService, statistics,
              startTime);
      executorService.schedule(runnable, _random.nextInt(testDelay), TimeUnit.MILLISECONDS);
    }
    Thread.sleep(_runTime);
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.SECONDS);
    if (!executorService.isTerminated()) {
      LOG.warn("executor service not terminated");
      executorService.shutdownNow();
    }
    try {
      _queryExecutor.close(context);
    } catch (Exception e) {
      throw new RuntimeException("failed to close query executor " + _queryExecutor, e);
    }
    return new LoadTestNodeOperationResult(context.getNode().getName(), statistics);
  }

  private final class TestSearcherRunnable implements Runnable {
    private int _queryIndex;
    private int _testDelay;
    private final NodeContext _context;
    private final ScheduledExecutorService _executorService;
    private final List<LoadTestQueryResult> _statistics;
    private final long _startTime;

    TestSearcherRunnable(int testDelay, NodeContext context, ScheduledExecutorService executorService,
            List<LoadTestQueryResult> statistics, long startTime) {
      _testDelay = testDelay;
      _context = context;
      _executorService = executorService;
      _statistics = statistics;
      _startTime = startTime;
      _queryIndex = _random.nextInt(_queryExecutor.getQueries().length);
    }

    @Override
    public void run() {
      long queryStartTime = System.currentTimeMillis();
      String queryString = _queryExecutor.getQueries()[_queryIndex];
      _queryIndex = (_queryIndex + 1) % _queryExecutor.getQueries().length;
      try {
        _queryExecutor.execute(_context, queryString);
        _statistics.add(new LoadTestQueryResult(queryStartTime, System.currentTimeMillis(), queryString, _context
                .getNode().getName()));
      } catch (Exception e) {
        _statistics.add(new LoadTestQueryResult(queryStartTime, -1, queryString, _context.getNode().getName()));
        LOG.error("Search failed.", e);
      }
      long now = System.currentTimeMillis();
      if (now - _startTime < _runTime - _testDelay) {
        int testDelay = Math.max(0, (int) (_testDelay - (now - queryStartTime)));
        _executorService.schedule(this, _random.nextInt(testDelay * 2), TimeUnit.MILLISECONDS);
      }
    }
  }

}
