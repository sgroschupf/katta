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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.OperationResult;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

/**
 * When watchdog for a list of {@link NodeOperation}s. The watchdog is finished
 * if all operations are done or the nodes of the incomplete nodes went down.
 */
public class OperationWatchdog implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(OperationWatchdog.class);

  private final List<OperationId> _openOperationIds;
  private final List<OperationId> _operationIds;
  private final LeaderContext _context;
  private final LeaderOperation _leaderOperation;

  public OperationWatchdog(LeaderContext context, List<OperationId> operationIds, LeaderOperation leaderOperation) {
    _context = context;
    _operationIds = operationIds;
    _openOperationIds = new ArrayList<OperationId>(operationIds);
    _leaderOperation = leaderOperation;
    subscribeNotifications();
  }

  private final synchronized void subscribeNotifications() {
    checkDeploymentForCompletion();
    if (isDone()) {
      return;
    }

    InteractionProtocol protocol = _context.getProtocol();
    protocol.registerNodeListener(this, new IAddRemoveListener() {
      @Override
      public void removed(String name) {
        checkDeploymentForCompletion();
      }

      @Override
      public void added(String name) {
        // nothing todo
      }
    });
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataDeleted(String arg0) throws Exception {
        checkDeploymentForCompletion();
      }

      @Override
      public void handleDataChange(String arg0, Serializable arg1) throws Exception {
        // nothing todo
      }
    };
    for (OperationId operationId : _openOperationIds) {
      protocol.registerNodeOperationListener(this, operationId, dataListener);
    }
    checkDeploymentForCompletion();
  }

  protected final synchronized void checkDeploymentForCompletion() {
    if (isDone()) {
      return;
    }

    InteractionProtocol protocol = _context.getProtocol();
    List<String> liveNodes = protocol.getLiveNodes();
    for (Iterator iter = _openOperationIds.iterator(); iter.hasNext();) {
      OperationId operationId = (OperationId) iter.next();
      if (!protocol.isNodeOperationQueued(operationId) || !liveNodes.contains(operationId.getNodeName())) {
        iter.remove();
      }
    }
    if (isDone()) {
      finishWatchdog(protocol);
    } else {
      LOG.debug("still " + getOpenOperationCount() + " open deploy operations");
    }
  }

  public synchronized void cancel() {
    _context.getProtocol().unregisterComponent(this);
    this.notifyAll();
  }

  private synchronized void finishWatchdog(InteractionProtocol protocol) {
    protocol.unregisterComponent(this);
    try {
      List<OperationResult> operationResults = new ArrayList<OperationResult>(_openOperationIds.size());
      for (OperationId operationId : _operationIds) {
        OperationResult operationResult = protocol.getNodeOperationResult(operationId, true);
        if (operationResult != null && operationResult.getUnhandledException() != null) {
          // TODO jz: do we need to inform the master operation ?
          LOG.error("received unhandlde exception from node " + operationId.getNodeName(), operationResult
                  .getUnhandledException());
        }
        operationResults.add(operationResult);// we add null ones
      }
      _leaderOperation.nodeOperationsComplete(_context, operationResults);
    } catch (Exception e) {
      LOG.info("operation complete action of " + _leaderOperation + " failed", e);
    }
    LOG.info("watch for " + _leaderOperation + " finished");
    this.notifyAll();
  }

  public LeaderOperation getOperation() {
    return _leaderOperation;
  }

  public final int getOpenOperationCount() {
    return _openOperationIds.size();
  }

  public boolean isDone() {
    return _openOperationIds.isEmpty();
  }

  public final synchronized void join() throws InterruptedException {
    join(0);
  }

  public final synchronized void join(long timeout) throws InterruptedException {
    if (!isDone()) {
      this.wait(timeout);
    }
  }

  @Override
  public final void disconnect() {
    // handled by leader
  }

  @Override
  public final void reconnect() {
    // handled by leader
  }

}
