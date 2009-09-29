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
package net.sf.katta.node;

import junit.framework.TestCase;

public class LuceneServerTest extends TestCase {

  public void testPriorityQueue() throws Exception {
    // tests some simple PriorityQueue behavior
    LuceneServer.KattaHitQueue queue = new LuceneServer().new KattaHitQueue(2);
    Hit hit1 = new Hit("shard", "node", 1f, 1);
    Hit hit2 = new Hit("shard", "node", 2f, 1);
    Hit hit3 = new Hit("shard", "node", 3f, 1);
    Hit hit4 = new Hit("shard", "node", 4f, 1);

    assertTrue(queue.insert(hit1));
    assertTrue(queue.insert(hit2));
    assertTrue(queue.insert(hit3));
    assertTrue(queue.insert(hit4));
    
    assertEquals(2, queue.size());
    assertSame(hit3, queue.pop());
    assertSame(hit4, queue.pop());
  }

  public void testPriorityQueue_sameScore() throws Exception {
    LuceneServer.KattaHitQueue queue = new LuceneServer().new KattaHitQueue(2);
    Hit hit1 = new Hit("shard", "node", 1f, 1);
    Hit hit2 = new Hit("shard", "node", 1f, 2);
    Hit hit3 = new Hit("shard", "node", 1f, 3);

    assertTrue(queue.insert(hit1));
    assertTrue(queue.insert(hit2));
    assertTrue(queue.insert(hit3));
    assertEquals(2, queue.size());

    // Queue should return documents with the smaller document ids first if
    // documents have the same score.
    assertSame(hit2, queue.pop());
    assertSame(hit3, queue.pop());
  }
}
