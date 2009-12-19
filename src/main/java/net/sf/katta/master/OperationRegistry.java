package net.sf.katta.master;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;

import org.apache.log4j.Logger;

//TODO jz: persist into zk for master change safety ?
public class OperationRegistry {

  private final static Logger LOG = Logger.getLogger(OperationRegistry.class);

  private final LeaderContext _context;
  private final List<OperationWatchdog> _watchdogs = new ArrayList<OperationWatchdog>();

  public OperationRegistry(LeaderContext context) {
    _context = context;
  }

  public synchronized OperationWatchdog watchFor(List<OperationId> operationIds, LeaderOperation operation) {
    LOG.info("watch operation " + operation + " for node operations " + operationIds);
    releaseDoneWatchdogs(); // lazy cleaning
    OperationWatchdog watchdog = new OperationWatchdog(_context, operationIds, operation);
    _watchdogs.add(watchdog);
    return watchdog;
  }

  private void releaseDoneWatchdogs() {
    for (Iterator iterator = _watchdogs.iterator(); iterator.hasNext();) {
      OperationWatchdog watchdog = (OperationWatchdog) iterator.next();
      if (watchdog.isDone()) {
        iterator.remove();
      }
    }
  }

  public synchronized boolean isLocked(LeaderOperation operation) {
    for (Iterator iterator = _watchdogs.iterator(); iterator.hasNext();) {
      OperationWatchdog watchdog = (OperationWatchdog) iterator.next();
      if (watchdog.isDone()) {
        iterator.remove(); // lazy cleaning
      } else if (watchdog.getOperation().locksOperation(operation)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void shutdown() {
    for (Iterator iterator = _watchdogs.iterator(); iterator.hasNext();) {
      OperationWatchdog watchdog = (OperationWatchdog) iterator.next();
      watchdog.cancel();
      iterator.remove();
    }
  }

}
