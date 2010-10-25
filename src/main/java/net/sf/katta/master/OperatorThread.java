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

import java.util.List;

import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.master.MasterOperation.ExecutionInstruction;
import net.sf.katta.protocol.MasterQueue;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.log4j.Logger;

/**
 * Responsible for executing all {@link MasterOperation}s which are offered to
 * the master queue sequentially.
 */
class OperatorThread extends Thread {

  protected final static Logger LOG = Logger.getLogger(OperatorThread.class);

  private final MasterContext _context;
  private final MasterQueue _queue;
  private final OperationRegistry _registry;
  private final long _safeModeMaxTime;

  private boolean _safeMode = true;

  public OperatorThread(final MasterContext context, final long safeModeMaxTime) {
    _context = context;
    _queue = context.getMasterQueue();
    _registry = new OperationRegistry(context);
    setDaemon(true);
    setName(getClass().getSimpleName());
    _safeModeMaxTime = safeModeMaxTime;
  }

  public boolean isInSafeMode() {
    return _safeMode;
  }

  public OperationRegistry getOperationRegistry() {
    return _registry;
  }

  public MasterContext getContext() {
    return _context;
  }

  @Override
  public void run() {
    try {
      LOG.info("starting...");
      runInSafeMode();
      recreateWatchdogs();

      while (true) {
        try {
          // TODO jz: poll only for a certain amount of time and then execute a
          // global check operation ?
          MasterOperation operation = _queue.peek();
          List<OperationId> nodeOperationIds = null;
          try {
            List<MasterOperation> runningOperations = _registry.getRunningOperations();
            ExecutionInstruction instruction = operation.getExecutionInstruction(runningOperations);
            nodeOperationIds = executeOperation(operation, instruction, runningOperations);
          } catch (Exception e) {
            ExceptionUtil.rethrowInterruptedException(e);
            LOG.error("failed to execute " + operation, e);
          }
          if (nodeOperationIds != null && !nodeOperationIds.isEmpty()) {
            OperationWatchdog watchdog = _queue.moveOperationToWatching(operation, nodeOperationIds);
            _registry.watchFor(watchdog);
          } else {
            _queue.remove();
          }
        } catch (Throwable e) {
          ExceptionUtil.rethrowInterruptedException(e);
          LOG.fatal("master operation failure", e);
        }
      }
    } catch (final InterruptedException e) {
      Thread.interrupted();
      // let go the thread
    } catch (ZkInterruptedException e) {
      Thread.interrupted();
      // let go the thread
    }
    _registry.shutdown();
    LOG.info("operator thread stopped");
  }

  private void recreateWatchdogs() {
    List<OperationWatchdog> watchdogs = _context.getMasterQueue().getWatchdogs();
    for (OperationWatchdog watchdog : watchdogs) {
      if (watchdog.isDone()) {
        LOG.info("release done watchdog " + watchdog);
        _queue.removeWatchdog(watchdog);
      } else {
        _registry.watchFor(watchdog);
      }
    }
  }

  private List<OperationId> executeOperation(MasterOperation operation, ExecutionInstruction instruction,
          List<MasterOperation> runningOperations) throws Exception {
    List<OperationId> operationIds = null;
    switch (instruction) {
    case EXECUTE:
      LOG.info("executing operation '" + operation + "'");
      operationIds = operation.execute(_context, runningOperations);
      break;
    case CANCEL:
      // just do nothing
      LOG.info("skipping operation '" + operation + "'");
      break;
    case ADD_TO_QUEUE_TAIL:
      LOG.info("adding operation '" + operation + "' to end of queue");
      _queue.add(operation);
      break;
    default:
      throw new IllegalStateException("execution instruction " + instruction + " not handled");
    }
    return operationIds;
  }

  private void runInSafeMode() throws InterruptedException {
    _safeMode = true;
    // List<String> knownNodes = _protocol.getKnownNodes(); //TODO jz: use known
    // nodes ?
    List<String> previousLiveNodes = _context.getProtocol().getLiveNodes();
    long lastChange = System.currentTimeMillis();
    try {
      while (previousLiveNodes.isEmpty() || lastChange + _safeModeMaxTime > System.currentTimeMillis()) {
        LOG.info("SAFE MODE: No nodes available or state unstable within the last " + _safeModeMaxTime + " ms.");
        Thread.sleep(_safeModeMaxTime / 4);// TODO jz: listen on life nodes ?

        List<String> currentLiveNodes = _context.getProtocol().getLiveNodes();
        if (currentLiveNodes.size() != previousLiveNodes.size()) {
          lastChange = System.currentTimeMillis();
          previousLiveNodes = currentLiveNodes;
        }
      }
      LOG.info("SAFE MODE: leaving safe mode with " + previousLiveNodes.size() + " connected nodes");
    } finally {
      _safeMode = false;
    }
  }

}
