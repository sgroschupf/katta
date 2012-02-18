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
package net.sf.katta.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.util.KattaException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A multithreaded destination for results and/or errors. Results are produced
 * by nodes and we pass lists of shards to nodes. But due to replication and
 * retries, we associate sets of shards with the results, not nodes.
 * 
 * Multiple NodeInteractions will be writing to this object at the same time. If
 * not closed, expect the contents to change. For example isComplete() might
 * return false and then a call to getResults() might return a complete set (in
 * which case another call to isComplete() would return true). If you need
 * complex state information, rather than making multiple calls, you should use
 * 
 * 
 * 
 * You can get these results from a WorkQueue by polling or blocking. Once you
 * have an ClientResult instance you may poll it or block on it. Whenever
 * resutls or errors are added notifyAll() is called. The ClientResult can
 * report on the number or ratio of shards completed. You can stop the search by
 * calling close(). The ClientResult will no longer change, and any outstanding
 * threads will be killed (via notification to the provided IClosedListener).
 */
public class ClientResult<T> implements IResultReceiver<T>, Iterable<ClientResult<T>.Entry> {

  private static final Logger LOG = Logger.getLogger(ClientResult.class);

  /**
   * Immutable storage of either a result or an error, which shards produced it,
   * and it's arrival time.
   */
  public class Entry {

    public final T result;
    public final Throwable error;
    public final Set<String> shards;
    public final long time;

    @SuppressWarnings("unchecked")
    private Entry(Object o, Collection<String> shards, boolean isError) {
      this.result = !isError ? (T) o : null;
      this.error = isError ? (Throwable) o : null;
      this.shards = Collections.unmodifiableSet(new HashSet<String>(shards));
      this.time = System.currentTimeMillis();
    }

    @Override
    public String toString() {
      String resultStr;
      if (result != null) {
        resultStr = "null";
        if (result != null) {
          try {
            resultStr = result.toString();
          } catch (Throwable t) {
            LOG.trace("Error calling toString() on result", t);
            resultStr = "(toString() err)";
          }
        }
        if (resultStr == null) {
          resultStr = "(null toString())";
        }
      } else {
        resultStr = error != null ? error.getClass().getSimpleName() : "null";
      }
      return String.format("%s from %s at %d", resultStr, shards, time);
    }
  }

  /**
   * Provides a way to notify interested parties when our close() method is
   * called.
   */
  public interface IClosedListener {
    /**
     * The ClientResult's close() method was called. The result is closed before
     * calling this.
     */
    public void clientResultClosed();
  }

  private boolean closed = false;
  private final Set<String> allShards;
  private final Set<String> seenShards = new HashSet<String>();
  private final Set<Entry> entries = new HashSet<Entry>();
  private final Map<Object, Entry> resultMap = new HashMap<Object, Entry>();
  private final Collection<T> results = new ArrayList<T>();
  private final Collection<Throwable> errors = new ArrayList<Throwable>();
  private final long startTime = System.currentTimeMillis();
  private final IClosedListener closedListener;

  /**
   * Construct a non-closed ClientResult, which waits for addResults() or
   * addError() calls until close() is called. After that point, addResults()
   * and addError() calls are ignored, and this object becomes immutable.
   * 
   * @param closedListener
   *          If not null, it's clientResultClosed() method is called when our
   *          close() method is.
   * @param allShards
   *          The set of all shards to expect results from.
   */
  public ClientResult(IClosedListener closedListener, Collection<String> allShards) {
    if (allShards == null || allShards.isEmpty()) {
      throw new IllegalArgumentException("No shards specified");
    }
    this.allShards = Collections.unmodifiableSet(new HashSet<String>(allShards));
    this.closedListener = closedListener;
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Created ClientResult(%s, %s)", closedListener != null ? closedListener : "null",
              allShards));
    }
  }

  /**
   * Construct a non-closed ClientResult, which waits for addResults() or
   * addError() calls until close() is called. After that point, addResults()
   * and addError() calls are ignored, and this object becomes immutable.
   * 
   * @param closedListener
   *          If not null, it's clientResultClosed() method is called when our
   *          close() method is.
   * @param allShards
   *          The set of all shards to expect results from.
   */
  public ClientResult(IClosedListener closedListener, String... allShards) {
    this(closedListener, Arrays.asList(allShards));
  }

  /**
   * Add a result. Will be ignored if closed.
   * 
   * @param result
   *          The result to add.
   * @param shards
   *          The shards used to compute the result.
   */
  public void addResult(T result, Collection<String> shards) {
    if (closed) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Ignoring results given to closed ClientResult");
      }
      return;
    }
    if (shards == null) {
      LOG.warn("Null shards passed to AddResult()");
      return;
    }
    Entry entry = new Entry(result, shards, false);
    if (entry.shards.isEmpty()) {
      LOG.warn("Empty shards passed to AddResult()");
      return;
    }
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Adding result %s", entry));
      }
      if (LOG.isEnabledFor(Level.WARN)) {
        for (String shard : entry.shards) {
          if (seenShards.contains(shard)) {
            LOG.warn("Duplicate occurances of shard " + shard);
          } else if (!allShards.contains(shard)) {
            LOG.warn("Unknown shard " + shard + " returned results");
          }
        }
      }
      entries.add(entry);
      seenShards.addAll(entry.shards);
      if (result != null) {
        results.add(result);
        resultMap.put(result, entry);
      }
      notifyAll();
    }
  }

  /**
   * Add a result. Will be ignored if closed.
   * 
   * @param result
   *          The result to add.
   * @param shards
   *          The shards used to compute the result.
   */
  public void addResult(T result, String... shards) {
    addResult(result, Arrays.asList(shards));
  }

  /**
   * Add an error. Will be ignored if closed.
   * 
   * @param error
   *          The error to add.
   * @param shards
   *          The shards used when the error happened.
   */
  public void addError(Throwable error, Collection<String> shards) {
    if (closed) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Ignoring exception given to closed ClientResult");
      }
      return;
    }
    if (shards == null) {
      LOG.warn("Null shards passed to addError()");
      return;
    }
    Entry entry = new Entry(error, shards, true);
    if (entry.shards.isEmpty()) {
      LOG.warn("Empty shards passed to addError()");
      return;
    }
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Adding error %s", entry));
      }
      if (LOG.isEnabledFor(Level.WARN)) {
        for (String shard : entry.shards) {
          if (seenShards.contains(shard)) {
            LOG.warn("Duplicate occurances of shard " + shard);
          } else if (!allShards.contains(shard)) {
            LOG.warn("Unknown shard " + shard + " returned results");
          }
        }
      }
      entries.add(entry);
      seenShards.addAll(entry.shards);
      if (error != null) {
        errors.add(error);
        resultMap.put(error, entry);
      }
      notifyAll();
    }
  }

  /**
   * Add an error. Will be ignored if closed.
   * 
   * @param error
   *          The error to add.
   * @param shards
   *          The shards used when the error happened.
   */
  public synchronized void addError(Throwable error, String... shards) {
    addError(error, Arrays.asList(shards));
  }

  /**
   * Stop accepting additional results or errors. Become an immutable object.
   * Also report the closure to the IClosedListener passed to our constructor,
   * if any. Normally this will tell the WorkQueue to shut down immediately,
   * killing any still running threads.
   */
  public synchronized void close() {
    LOG.trace("close() called.");
    if (!closed) {
      closed = true;
      if (closedListener != null) {
        LOG.trace("Notifying closed listener.");
        closedListener.clientResultClosed();
      }
    }
    notifyAll();
  }

  /**
   * Is this result set closed, and therefore not accepting any additional
   * results or errors. Once closed, this becomes an immutable object.
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * @return the set of all shards we are expecting results from.
   */
  public Set<String> getAllShards() {
    return allShards;
  }

  /**
   * @return the set of shards from whom we have seen either results or errors.
   */
  public synchronized Set<String> getSeenShards() {
    return Collections.unmodifiableSet(closed ? seenShards : new HashSet<String>(seenShards));
  }

  /**
   * @return the subset of all shards from whom we have not seen either results
   *         or errors.
   */
  public synchronized Set<String> getMissingShards() {
    Set<String> missing = new HashSet<String>(allShards);
    missing.removeAll(seenShards);
    return missing;
  }

  /**
   * @return all of the results seen so far. Does not include errors.
   */
  public synchronized Collection<T> getResults() {
    return Collections.unmodifiableCollection(closed ? results : new ArrayList<T>(results));
  }

  /**
   * Either return results or throw an exception. Allows simple one line use of
   * a ClientResult. If no errors occurred, returns same results as
   * getResults(). If any errors occurred, one is chosen via getError() and
   * thrown.
   * 
   * @return if no errors occurred, results via getResults().
   * @throws Throwable
   *           if any errors occurred, via getError().
   */
  public synchronized Collection<T> getResultsOrThrowException() throws Throwable {
    if (isError()) {
      throw getError();
    } else {
      return getResults();
    }
  }

  /**
   * Either return results or throw a KattaException. Allows simple one line use
   * of a ClientResult. If no errors occurred, returns same results as
   * getResults(). If any errors occurred, one is chosen via getKattaException()
   * and thrown.
   * 
   * @return if no errors occurred, results via getResults().
   * @throws KattaException
   *           if any errors occurred, via getError().
   */
  public synchronized Collection<T> getResultsOrThrowKattaException() throws KattaException {
    if (isError()) {
      throw getKattaException();
    } else {
      return getResults();
    }
  }

  /**
   * @return all of the errors seen so far.
   */
  public synchronized Collection<Throwable> getErrors() {
    return Collections.unmodifiableCollection(closed ? errors : new ArrayList<Throwable>(errors));
  }

  /**
   * @return a randomly chosen error, or null if none exist.
   */
  public synchronized Throwable getError() {
    for (Entry e : entries) {
      if (e.error != null) {
        return e.error;
      }
    }
    return null;
  }

  /**
   * @return a randomly chosen KattaException if one exists, else a
   *         KattaException wrapped around a randomly chosen error if one
   *         exists, else null.
   */
  public synchronized KattaException getKattaException() {
    Throwable error = null;
    for (Entry e : this) {
      if (e.error != null) {
        if (e.error instanceof KattaException) {
          return (KattaException) e.error;
        } else {
          error = e.error;
        }
      }
    }
    if (error != null) {
      return new KattaException("Error", error);
    } else {
      return null;
    }
  }

  /**
   * @param result
   *          The result to look up.
   * @return What shards produced the result, and when it arrived. Returns null
   *         if result not found.
   */
  public synchronized Entry getResultEntry(T result) {
    return resultMap.get(result);
  }

  /**
   * @param error
   *          The error to look up.
   * @return What shards produced the error, and when it arrived. Returns null
   *         if error not found.
   */
  public synchronized Entry getErrorEntry(Throwable error) {
    return resultMap.get(error);
  }

  /**
   * @return true if we have seen either a result or an error for all shards.
   */
  public synchronized boolean isComplete() {
    return seenShards.containsAll(allShards);
  }

  /**
   * @return true if any errors were reported.
   */
  public synchronized boolean isError() {
    return !errors.isEmpty();
  }

  /**
   * @return true if result is complete (all shards reporting in) and no errors
   *         occurred.
   */
  public synchronized boolean isOK() {
    return isComplete() && !isError();
  }

  /**
   * @return the ratio (0.0 .. 1.0) of shards we have seen. 0.0 when no shards,
   *         1.0 when complete.
   */
  public synchronized double getShardCoverage() {
    int seen = seenShards.size();
    int all = allShards.size();
    return all > 0 ? (double) seen / (double) all : 0.0;
  }

  /**
   * @return the time when this ClientResult was created.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * @return a snapshot of all the data about the results so far.
   */
  public Set<Entry> entrySet() {
    if (closed) {
      return Collections.unmodifiableSet(entries);
    } else {
      synchronized (this) {
        // Set will keep changing, make a snapshot.
        return Collections.unmodifiableSet(new HashSet<Entry>(entries));
      }
    }
  }

  /**
   * @return an iterator of our Entries sees so far.
   */
  public Iterator<Entry> iterator() {
    return entrySet().iterator();
  }

  /**
   * @return a list of our results or errors, in the order they arrived.
   */
  public List<Entry> getArrivalTimes() {
    List<Entry> arrivals;
    synchronized (this) {
      arrivals = new ArrayList<Entry>(entries);
    }
    Collections.sort(arrivals, new Comparator<Entry>() {
      public int compare(Entry o1, Entry o2) {
        if (o1.time != o2.time) {
          return o1.time < o2.time ? -1 : 1;
        } else {
          // Break ties in favor of results.
          if (o1.result != null && o2.result == null) {
            return -1;
          } else if (o2.result != null && o1.result == null) {
            return 1;
          } else {
            return 0;
          }
        }
      }
    });
    return arrivals;
  }

  public void waitFor(IResultPolicy<T> policy) {
    long waitTime = 0;
    while (true) {
      synchronized (results) {
        // Need to stay synchronized before waitTime() through wait() or we will
        // miss notifications.
        waitTime = policy.waitTime(this);
        if (waitTime > 0 && !closed) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Waiting %d ms, results = %s", waitTime, this));
          }
          try {
            synchronized (this) {
              this.wait(waitTime);
            }
          } catch (InterruptedException e) {
            LOG.debug("Interrupted", e);
          }
          if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Done waiting, results = %s", this));
          }
        } else {
          break;
        }
      }
    }
    if (waitTime < 0) {
      close();
    }
  }

  @Override
  public synchronized String toString() {
    int numResults = 0;
    int numErrors = 0;
    for (Entry e : this) {
      if (e.result != null) {
        numResults++;
      }
      if (e.error != null) {
        numErrors++;
      }
    }
    return String.format("ClientResult: %d results, %d errors, %d/%d shards%s%s", numResults, numErrors, seenShards
            .size(), allShards.size(), closed ? " (closed)" : "", isComplete() ? " (complete)" : "");
  }

}
