/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.slave;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Writable;

public class DocumentFrequenceWritable implements Writable {
  // TODO decide which one is better
  // NonBlockingHashMap<TermWritable, Integer> _frequencies = new
  // NonBlockingHashMap<TermWritable, Integer>();
  Map<TermWritable, Integer> _frequencies = new ConcurrentHashMap<TermWritable, Integer>();

  private int _numDocs;

  public void put(final String field, final String term, final int frequency) {
    add(new TermWritable(field, term), frequency);
  }

  private void add(final TermWritable key, final int frequency) {
    int result = frequency;
    final Integer frequencyObject = _frequencies.get(key);
    if (frequencyObject != null) {
      result += frequencyObject.intValue();
    }
    _frequencies.put(key, result);
  }

  public void putAll(final Map<TermWritable, Integer> frequencyMap) {
    final Set<TermWritable> keySet = frequencyMap.keySet();
    for (final TermWritable key : keySet) {
      add(key, frequencyMap.get(key).intValue());
    }
  }

  public Integer get(final String field, final String term) {
    return get(new TermWritable(field, term));
  }

  public void addNumDocs(final int numDocs) {
    _numDocs += numDocs;
  }

  public Integer get(final TermWritable key) {
    return _frequencies.get(key);
  }

  public Map<TermWritable, Integer> getAll() {
    return _frequencies;
  }

  public void readFields(final DataInput in) throws IOException {
    final int size = in.readInt();
    for (int i = 0; i < size; i++) {
      final TermWritable term = new TermWritable();
      term.readFields(in);
      final int frequency = in.readInt();
      _frequencies.put(term, frequency);
    }
    _numDocs = in.readInt();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(_frequencies.size());
    for (final TermWritable key : _frequencies.keySet()) {
      key.write(out);
      final Integer frequency = _frequencies.get(key);
      out.writeInt(frequency);
    }
    out.writeInt(_numDocs);
  }

  public int getNumDocs() {
    return _numDocs;
  }

  public void setNumDocs(final int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public String toString() {
    return "numDocs: " + getNumDocs() + getAll();
  }

}
