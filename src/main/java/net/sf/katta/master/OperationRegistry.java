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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;

import org.apache.log4j.Logger;

//TODO jz: persist into zk for master change safety ?
public class OperationRegistry {

  private final static Logger LOG = Logger.getLogger(OperationRegistry.class);

  private final MasterContext _context;
  private final List<OperationWatchdog> _watchdogs = new ArrayList<OperationWatchdog>();

  public OperationRegistry(MasterContext context) {
    _context = context;
  }

  public synchronized OperationWatchdog watchFor(List<OperationId> operationIds, MasterOperation operation) {
    LOG.info("watch operation '" + operation + "' for node operations " + operationIds);
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

  public synchronized List<MasterOperation> getRunningOperations() {
    List<MasterOperation> operations = new ArrayList<MasterOperation>();
    for (Iterator iterator = _watchdogs.iterator(); iterator.hasNext();) {
      OperationWatchdog watchdog = (OperationWatchdog) iterator.next();
      if (watchdog.isDone()) {
        iterator.remove(); // lazy cleaning
      } else {
        operations.add(watchdog.getOperation());
      }
    }
    return operations;
  }

  public synchronized void shutdown() {
    for (Iterator iterator = _watchdogs.iterator(); iterator.hasNext();) {
      OperationWatchdog watchdog = (OperationWatchdog) iterator.next();
      watchdog.cancel();
      iterator.remove();
    }
  }

}
