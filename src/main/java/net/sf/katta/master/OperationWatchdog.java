package net.sf.katta.master;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.node.NodeOperation;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

/**
 * When watchdog for a list of {@link NodeOperation}s. The watchdog is finished
 * if all operations are done or the nodes of the incomplete nodes went down.
 */
public class OperationWatchdog implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(OperationWatchdog.class);

  private final List<OperationId> _deploymentOperationIds;
  private final LeaderContext _context;
  private final LeaderOperation _leaderOperation;

  public OperationWatchdog(LeaderContext context, List<OperationId> deploymentOperationIds,
          LeaderOperation leaderOperation) {
    _context = context;
    _deploymentOperationIds = deploymentOperationIds;
    _leaderOperation = leaderOperation;
    subscribeNotifications();
  }

  private final synchronized void subscribeNotifications() {
    checkDeploymentForCompletion();
    if (isDone()) {
      return;
    }

    InteractionProtocol protocol = _context.getProtocol();
    protocol.registerMetricsNodeListener(this, new IAddRemoveListener() {
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
    for (OperationId operationId : _deploymentOperationIds) {
      protocol.registerNodeOperationListener(this, operationId, dataListener);
    }
    checkDeploymentForCompletion();
  }

  protected final synchronized void checkDeploymentForCompletion() {
    if (isDone()) {
      return;
    }

    InteractionProtocol protocol = _context.getProtocol();
    List<String> liveNodes = protocol.getNodes();
    for (Iterator iter = _deploymentOperationIds.iterator(); iter.hasNext();) {
      OperationId operationId = (OperationId) iter.next();
      if (!protocol.isNodeOperationQueued(operationId) || !liveNodes.contains(operationId.getNodeName())) {
        iter.remove();
      }
    }
    if (isDone()) {
      protocol.unregisterComponent(this);
      try {
        _leaderOperation.nodeOperationsComplete(_context);
      } catch (Exception e) {
        LOG.info("operation complete action of " + _leaderOperation + " failed", e);
      }
      LOG.info("watch for " + _leaderOperation + " finished");
      this.notifyAll();
    } else {
      LOG.debug("still " + getOpenOperationCount() + " open deploy operations");
    }
  }

  public LeaderOperation getOperation() {
    return _leaderOperation;
  }

  public final int getOpenOperationCount() {
    return _deploymentOperationIds.size();
  }

  public boolean isDone() {
    return _deploymentOperationIds.isEmpty();
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
