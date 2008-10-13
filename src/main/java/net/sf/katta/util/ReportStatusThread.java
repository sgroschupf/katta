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
package net.sf.katta.util;

import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Thread which periodically reports to a hadoop {@link Reporter}. This is
 * needed for long taking map/reduce-task which do not write to the output
 * collector, since hadoop kills task which do not write output or report their
 * status for a configured amount of time.
 */
public class ReportStatusThread extends Thread {

  protected final static Logger LOG = Logger.getLogger(ReportStatusThread.class);

  private final Reporter _reporter;
  private final String _statusString;
  private final long _reportIntervall;

  public ReportStatusThread(Reporter reporter, String statusString, long reportIntervall) {
    _reporter = reporter;
    _statusString = statusString;
    _reportIntervall = reportIntervall;
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (true) {
        _reporter.setStatus(_statusString);
        sleep(_reportIntervall);
      }
    } catch (final InterruptedException e) {
      LOG.debug("status thread " + _statusString + " stopped");
    }
  }

  public static ReportStatusThread startStatusThread(Reporter reporter, String statusString, long reportIntervall) {
    ReportStatusThread reportStatusThread = new ReportStatusThread(reporter, statusString, reportIntervall);
    reportStatusThread.start();
    return reportStatusThread;
  }

}
