package net.sf.katta.lib.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds an IndexReader and IndexSearcher and maintains the current number of threads using
 * it. For every call to getSearcher(), finishSearcher() must be called
 * exactly one time. finally blocks are a good idea.
 */
public class IndexHandle {
  private volatile IndexReader _indexReader;
  private volatile IndexSearcher _indexSearcher;
  private final Object _lock = new Object();
  private final AtomicInteger _refCount = new AtomicInteger(0);

  private static final int INDEX_HANDLE_CLOSE_SLEEP_TIME = 500;

  public IndexHandle(IndexReader indexReader, IndexSearcher indexSearcher) {
    _indexReader = indexReader;
    _indexSearcher = indexSearcher;
  }

  /**
   * Returns the IndexSearcher and increments the usage count.
   * finishSearcher() must be called once after each call to getSearcher().
   *
   * @return the searcher
   */
  public IndexSearcher getSearcher() {
    synchronized (_lock) {
      if (_refCount.get() < 0) {
        return null;
      }
      _refCount.incrementAndGet();
    }
    return _indexSearcher;
  }

  /**
   * Decrements the searcher usage count.
   */
  public void finishSearcher() {
    synchronized (_lock) {
      _refCount.decrementAndGet();
    }
  }

  /**
   * Spins until the searcher is no longer in use, then closes it.
   *
   * @throws java.io.IOException
   *           on IndexSearcher close failure
   */
  public void closeSearcher() throws IOException {
    while (true) {
      synchronized (_lock) {
        if (_refCount.get() == 0) {
          IndexReader indexReader = _indexReader;
          _indexSearcher = null;
          _indexReader = null;

          _refCount.set(-1);

          indexReader.close();
          return;
        }
      }
      try {
        Thread.sleep(INDEX_HANDLE_CLOSE_SLEEP_TIME);
      } catch (InterruptedException e) {
      }
    }
  }
}
