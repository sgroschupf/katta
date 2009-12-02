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
package net.sf.katta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Query;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

import org.I0Itec.zkclient.ZkClient;

public class PerformanceTest extends AbstractKattaTest {

  final int _hitCount = 200000;

  public static void main(final String[] args) throws InterruptedException, KattaException {
    final PerformanceTest p = new PerformanceTest();
    p.start();
  }

  private void start() throws InterruptedException, KattaException {
    startZkServer();
    MasterStartThread masterStartThread = startMaster();
    final ZkClient zkClientMaster = masterStartThread.getZkClient();

    NodeStartThread nodeStartThread1 = startNode(new LuceneServer());
    NodeStartThread nodeStartThread2 = startNode(new LuceneServer());
    masterStartThread.join();
    nodeStartThread1.join();
    nodeStartThread2.join();
    waitForChilds(zkClientMaster, _conf.getZKNodesPath(), 2);

    final Katta katta = new Katta(_conf);
    katta.addIndex("index1", TestResources.INDEX1.getAbsolutePath(), 1);
    katta.addIndex("index2", TestResources.INDEX2.getAbsolutePath(), 1);

    final ILuceneClient client = new LuceneClient(_conf);
    final Query query = new Query("foo: bar");
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      client.search(query, new String[] { "index2", "index1" });
    }
    System.out.println("search took: " + (System.currentTimeMillis() - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      client.count(query, new String[] { "index2", "index1" });
    }
    System.out.println("count took: " + (System.currentTimeMillis() - start));
    katta.close();
    client.close();
    nodeStartThread1.shutdown();
    nodeStartThread2.shutdown();
    masterStartThread.shutdown();
    stopZkServer();
  }

  public void testSortSpeed() {
    sortCollection(setupHits());
    sortMerge(setupHits());
    // sortOther(setupHits());//start with more memory
    sortOtherII(setupHits());
  }

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
