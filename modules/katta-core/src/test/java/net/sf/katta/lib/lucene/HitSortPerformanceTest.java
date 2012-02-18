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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.sf.katta.lib.lucene.Hit;
import net.sf.katta.lib.lucene.Hits;

import org.junit.Test;

public class HitSortPerformanceTest {

  final int _hitCount = 200000;

  @Test
  public void testSortSpeed() {
    sortCollection(setupHits());
    sortMerge(setupHits());
    // sortOther(setupHits());//start with more memory
    sortOtherII(setupHits());
  }

  @Test
  public void testSortSpeedWithSortedSublists() {
    sortCollection(setupHitsWithSortedSubLists());
    sortMerge(setupHitsWithSortedSubLists());
    // sortOther(setupHitsWithSortedSubLists());//start with more memory
    sortOtherII(setupHitsWithSortedSubLists());
  }

  public void sortCollection(Hits hits) {
    final long start = System.currentTimeMillis();
    hits.sortCollection(_hitCount);
    final long end = System.currentTimeMillis();
    System.out.println("sortCollection: " + (end - start) + "ms. for " + _hitCount);
  }

  public void sortMerge(Hits hits) {
    final long start = System.currentTimeMillis();
    hits.sortMerge();
    final long end = System.currentTimeMillis();
    System.out.println("sortMerge: " + (end - start) + "ms. for " + _hitCount);
  }

  public void sortOther(Hits hits) {
    final long start = System.currentTimeMillis();
    hits.sortOther();
    final long end = System.currentTimeMillis();
    System.out.println("sortOther: " + (end - start) + "ms. for " + _hitCount);
  }

  public void sortOtherII(Hits hits) {
    final long start = System.currentTimeMillis();
    hits.sortOtherII();
    final long end = System.currentTimeMillis();
    System.out.println("sortOtherII: " + (end - start) + "ms. for " + _hitCount);
  }

  private Hits setupHits() {
    final Random random = new Random();
    // the same number everytime to get comparable results
    random.setSeed(64567547657L);
    final List<Hit> hitList = new ArrayList<Hit>();
    for (int i = 0; i < _hitCount; i++) {
      final Hit hit = new Hit("shard", "node", random.nextFloat(), random.nextInt());
      hitList.add(hit);
    }

    final Hits hits = new Hits();
    hits.addHits(hitList);
    return hits;
  }

  private Hits setupHitsWithSortedSubLists() {
    final Random random = new Random();
    // the same number everytime to get comparable results
    random.setSeed(64567547657L);
    List<Hit>[] hitListsArray = new ArrayList[4];
    for (int i = 0; i < hitListsArray.length; i++) {
      hitListsArray[i] = new ArrayList<Hit>();
    }

    for (int i = 0; i < _hitCount; i++) {
      final Hit hit = new Hit("shard", "node", random.nextFloat(), random.nextInt());
      hitListsArray[random.nextInt(hitListsArray.length)].add(hit);
    }

    final Hits hits = new Hits();
    for (List<Hit> hitList : hitListsArray) {
      Collections.sort(hitList);
      hits.addHits(hitList);
    }
    return hits;
  }
}
