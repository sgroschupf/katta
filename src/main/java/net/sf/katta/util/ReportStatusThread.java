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
