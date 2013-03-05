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
package net.sf.katta.lib.lucene;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import com.google.common.base.Objects;

public class DocumentFrequencyWritable implements Writable {
  private ReadWriteLock _frequenciesLock = new ReentrantReadWriteLock(true);
  private Map<TermWritable, Integer> _frequencies = new HashMap<TermWritable, Integer>();

  private AtomicLong _numDocs = new AtomicLong();

  public void put(final String field, final String term, final int frequency) {
    _frequenciesLock.writeLock().lock();
    try {
      add(new TermWritable(field, term), frequency);
    } finally {
      _frequenciesLock.writeLock().unlock();
    }
  }

  /**
   * Assumes a write lock is already in place.
   * 
   * @param key
   *          The item that has a frequency.
   * @param frequency
   *          The frequency of the key.
   */
  private void add(final TermWritable key, final int frequency) {
    int result = frequency;
    final Integer frequencyObject = _frequencies.get(key);
    if (frequencyObject != null) {
      result += frequencyObject;
    }
    _frequencies.put(key, result);
  }

  public void putAll(final Map<TermWritable, Integer> frequencyMap) {
    _frequenciesLock.writeLock().lock();
    try {
      final Set<TermWritable> keySet = frequencyMap.keySet();
      for (final TermWritable key : keySet) {
        add(key, frequencyMap.get(key));
      }
    } finally {
      _frequenciesLock.writeLock().unlock();
    }
  }

  public Integer get(final String field, final String term) {
    return get(new TermWritable(field, term));
  }

  public void addNumDocs(long numDocs) {
    if (Long.MAX_VALUE - numDocs - _numDocs.get() < 0) {
      Log.warn("max number of documents exceeded " + _numDocs.get() + " + " + numDocs);
      numDocs = Long.MAX_VALUE;
    }
    _numDocs.addAndGet(numDocs);
  }

  public Integer get(final TermWritable key) {
    _frequenciesLock.readLock().lock();
    try {
      return _frequencies.get(key);
    } finally {
      _frequenciesLock.readLock().unlock();
    }
  }

  public Map<TermWritable, Integer> getAll() {
    return Collections.unmodifiableMap(_frequencies);
  }

  public void readFields(final DataInput in) throws IOException {
    _frequenciesLock.writeLock().lock();
    try {
      final int size = in.readInt();
      for (int i = 0; i < size; i++) {
        final TermWritable term = new TermWritable();
        term.readFields(in);
        final int frequency = in.readInt();
        _frequencies.put(term, frequency);
      }
      _numDocs.set(in.readLong());
    } finally {
      _frequenciesLock.writeLock().unlock();
    }
  }

  public void write(final DataOutput out) throws IOException {
    _frequenciesLock.readLock().lock();
    try {
      out.writeInt(_frequencies.size());
      for (final TermWritable key : _frequencies.keySet()) {
        key.write(out);
        final Integer frequency = _frequencies.get(key);
        out.writeInt(frequency);
      }
      out.writeLong(_numDocs.get());
    } finally {
      _frequenciesLock.readLock().unlock();
    }
  }

  public long getNumDocs() {
    return _numDocs.get();
  }

  public int getNumDocsAsInteger() {
    if (_numDocs.get() > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) _numDocs.get();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("totalNumberOfDocs", getNumDocs()).add("termFrequencies", getAll())
            .toString();
  }
}
