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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.util.MergeSort;
import net.sf.katta.util.WritableType;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.Sort;

public class Hits implements Writable {

  private static final long serialVersionUID = -732226190122340208L;

  private List<List<Hit>> _hitsList = new Vector<List<Hit>>();

  private List<Hit> _sortedList;

  private AtomicInteger _totalHits = new AtomicInteger();

  public List<Hit> getHits() {
    if (_sortedList == null) {
      sort(Integer.MAX_VALUE);
    }
    return _sortedList;
  }

  public void addHits(final List<Hit> hits) {
    _hitsList.add(hits);
  }

  public void readFields(final DataInput in) throws IOException {
    // final long start = System.currentTimeMillis();
    final int listOfListsSize = in.readInt();
    _hitsList = new ArrayList<List<Hit>>(listOfListsSize);
    for (int i = 0; i < listOfListsSize; i++) {
      final int hitSize = in.readInt();
      final List<Hit> hitList = new ArrayList<Hit>(hitSize);
      for (int j = 0; j < hitSize; j++) {
        final Hit hit = new Hit();
        hit.readFields(in);
        hitList.add(hit);
      }
      _hitsList.add(hitList);

    }
    // final long end = System.currentTimeMillis();
    // Logger.info("Hits reading took " + (end - start) / 1000.0 + "sec.");
  }

  public void write(final DataOutput out) throws IOException {
    // final long start = System.currentTimeMillis();
    out.writeInt(_hitsList.size());
    for (final List<Hit> hitList : _hitsList) {
      out.writeInt(hitList.size());
      for (final Hit hit : hitList) {
        hit.write(out);
      }
    }
    // final long end = System.currentTimeMillis();
    // Logger.info("Hits writing took " + (end - start) / 1000.0 + "sec.");
  }

  public int size() {
    return _totalHits.get();
  }

  public void setTotalHits(final int totalHits) {
    _totalHits.set(totalHits);
  }

  public void sort(final int count) {
    sortCollection(count);
  }

  public void fieldSort(Sort sort, WritableType[] fieldTypes, int count) {
    // TODO merge sort does not work due KATTA-93
    final ArrayList<Hit> list = new ArrayList<Hit>(count);
    final int size = _hitsList.size();
    for (int i = 0; i < size; i++) {
      list.addAll(_hitsList.remove(0));
    }
    _hitsList = new ArrayList<List<Hit>>();
    if (!list.isEmpty()) {
      Collections.sort(list, new FieldSortComparator(sort.getSort(), fieldTypes));
    }
    _sortedList = list.subList(0, Math.min(count, list.size()));
  }

  @SuppressWarnings("unchecked")
  public void sortMerge() {
    final List<Hit>[] array = _hitsList.toArray(new List[_hitsList.size()]);
    _hitsList = new ArrayList<List<Hit>>();
    _sortedList = MergeSort.merge(array);
  }

  /*
   * Leads to OOM on 2 000 000 elements.
   */
  public void sortOther() {
    _sortedList = new ArrayList<Hit>();
    while (true) {
      Hit highest = null;
      final int[] pos = new int[_hitsList.size()];
      for (int i = 0; i < pos.length; i++) {
        pos[i] = 0;
      }
      int pointer = 0;
      for (int i = 0; i < _hitsList.size(); i++) {
        final List<Hit> list = _hitsList.get(i);
        if (list.size() > pos[i]) {
          final Hit hit = list.get(pos[i]);
          if (highest == null) {
            highest = hit;
            pointer = i;
          } else if (hit.getScore() > highest.getScore()) {
            highest = hit;
            pointer = i;
          }
        }
      }
      if (highest == null) {
        // no data anymore
        return;
      }
      pos[pointer]++;
      _sortedList.add(highest);
      highest = null;
    }
  }

  public void sortOtherII() {
    _sortedList = new ArrayList<Hit>();
    int pos = 0;
    while (true) {
      final List<Hit> tmp = new ArrayList<Hit>(_hitsList.size());
      for (final List<Hit> hitList : _hitsList) {
        if (hitList.size() > pos) {
          tmp.add(hitList.get(pos));
        }
      }
      if (tmp.size() == 0) {
        // we are done no new data
        return;
      }
      Collections.sort(tmp);
      _sortedList.addAll(tmp);
      pos++;
    }
  }

  /*
   * Leads on 10 000 000 list to OOM.
   */
  public void sortCollection(final int count) {
    final ArrayList<Hit> list = new ArrayList<Hit>();
    final int size = _hitsList.size();
    for (int i = 0; i < size; i++) {
      list.addAll(_hitsList.remove(0));
    }
    _hitsList = new ArrayList<List<Hit>>();
    Collections.sort(list);
    _sortedList = list.subList(0, Math.min(count, list.size()));
  }

  // public int compare(Hit o1, Hit o2) {
  // final float score2 = o2.getScore();
  // final float score1 = o1.getScore();
  // if (score1 > score2) {
  // return 1;
  // }
  // return -1;
  // }

  public void addTotalHits(final int size) {
    _totalHits.addAndGet(size);
  }

  @Override
  public String toString() {
    /*
     * Don't modify data structure just by viewing it, otherwise
     * running in a debugger modifies the behavior of the code!
     */
    return "Hits: total=" + _totalHits + ", queue=" + (_hitsList != null ? _hitsList.toString() : "null") +
      ", sorted=" + (_sortedList != null ? _sortedList.toString() : "null");
  }
  
}
