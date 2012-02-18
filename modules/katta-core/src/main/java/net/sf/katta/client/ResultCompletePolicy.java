/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.client;

/**
 * Wait for the results to be fully and/or partially complete (based on number
 * of shards), the result is closed, or N msec has passed, whichever comes
 * first. The resulting ClientResult may be closed or left open, depending on
 * the shutDown setting passed to the constructor. This class does not look at
 * the results themselves (type T), it only considers the number of shards
 * reporting (with a T result, or a Throwable if an error occured) compared to
 * the total number of shards.
 * 
 * If you must return in 5 seconds use new ResultCompletePolicy(5000). If you
 * want to do your own polling, use new ResultCompletePolicy(0, false). If you
 * want to wait a minimum of 3 seconds (but return sooner if results are
 * complete), then wait another 2 seconds for 95% coverage (shard based), then
 * use new ResultCompletePolicy(3000, 2000, 0.95, true).
 * 
 * You could also write a custom IResultPolicy that looks inside the result
 * objects to decide how much longer to wait.
 */
public class ResultCompletePolicy<T> implements IResultPolicy<T> {

  private final long completeWait;
  private final long completeStopTime;
  private final long coverageWait;
  private final long coverageStopTime;
  private final double coverage;
  private final boolean shutDown;

  /**
   * Wait for the results to be complete (all shards reporting with results or
   * errors) until result is complete, result is closed, or N msec has passed,
   * whichever comes first. Then close the result, shutting down the call.
   * 
   * @param timeout
   *          Max msec to wait for results.
   */
  public ResultCompletePolicy(long timeout) {
    this(timeout, 0, 1.0, true);
  }

  /**
   * Wait for the results to be complete (all shards reporting with results or
   * errors) until result is complete, result is closed, or N msec has passed,
   * whichever comes first. Then, if shutDown is true, close the result which
   * shuts down the call.
   * 
   * @param timeout
   * @param shutDown
   */
  public ResultCompletePolicy(long timeout, boolean shutDown) {
    this(timeout, 0, 1.0, shutDown);
  }

  /**
   * Wait for the results to complete (all shards reporting a result or error),
   * the results to be closed, or completeWait msec, whichever comes first. Then
   * if not complete and not closed, wait for the results to be closed, shard
   * coverage to be >= coverage, or coverageWait msec, whichever comes first. If
   * shutDown is set, close the result which terminates the call.
   * 
   * @param completeWait
   *          How long (msec) to wait for complete results.
   * @param coverageWait
   *          How long (msec, after completeWait) to wait for coverage to meet
   *          or exceed coverage param.
   * @param coverage
   *          The required coverage (0.0 .. 1.0) when waiting for coverage. Not
   *          used if coverageWait = 0.
   * @param shutDown
   *          Before returning the result, should it be closed.
   */
  public ResultCompletePolicy(long completeWait, long coverageWait, double coverage, boolean shutDown) {
    long now = System.currentTimeMillis();
    if (completeWait < 0 || coverageWait < 0) {
      throw new IllegalArgumentException("Wait times must be >= 0");
    }
    if (coverage < 0.0 || coverage > 1.0) {
      throw new IllegalArgumentException("Coverage must be 0.0 .. 1.0");
    }
    this.completeWait = completeWait;
    this.coverageWait = coverageWait;
    completeStopTime = now + completeWait;
    coverageStopTime = now + completeWait + coverageWait;
    this.coverage = coverage;
    this.shutDown = shutDown;
  }

  /**
   * How much longer, if any, should we wait for results to arrive. Also, should
   * WorkQueue be shut down, and the ClientResult closed?
   * 
   * @param result
   *          The results we have so far.
   * @return if > 0, sleep at most that many msec, or until a new result
   *         arrives, or the result is closed, whichever comes first. Then call
   *         this method again. If 0, stop waiting and return the result
   *         immediately. if < 0, shutdown the WorkQueue, close the result, and
   *         return it immediately.
   */
  public long waitTime(ClientResult<T> result) {
    boolean done = result.isClosed();
    long now = System.currentTimeMillis();
    if (!done) {
      if (now < completeStopTime) {
        done = result.isComplete();
      } else if (now < coverageStopTime) {
        done = result.getShardCoverage() >= coverage;
      } else {
        done = true;
      }
    }
    if (done) {
      return shutDown ? -1 : 0;
    }
    return coverageStopTime - now;
  }

  @Override
  public String toString() {
    String s = "Wait up to " + completeWait + " ms for complete results";
    if (coverageWait > 0) {
      s += ", then " + coverageWait + " ms for " + coverage + " coverage";
    }
    if (shutDown) {
      s += ", then shut down";
    }
    s += ".";
    return s;
  }

}
