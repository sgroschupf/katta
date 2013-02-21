package net.sf.katta.lib.lucene;

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LuceneServer_KattaHitQueueTest {
  
  @Test
  public void testPriorityQueue() throws Exception {
    // tests some simple PriorityQueue behavior
    LuceneServer.KattaHitQueue queue = new LuceneServer.KattaHitQueue(2);
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
  
  @Test
  public void testPriorityQueue_sameScore() throws Exception {
    LuceneServer.KattaHitQueue queue = new LuceneServer.KattaHitQueue(2);
    Hit hit1 = new Hit("shard", "node", 1f, 1);
    Hit hit2 = new Hit("shard", "node", 1f, 2);
    Hit hit3 = new Hit("shard", "node", 1f, 3);

    assertTrue(queue.insert(hit1));
    assertTrue(queue.insert(hit2));
    assertFalse(queue.insert(hit3));
    assertEquals(2, queue.size());

    // Queue should return documents with the smaller document ids first if
    // documents have the same score.
    assertSame(hit2, queue.pop());
    assertSame(hit1, queue.pop());
  }
  
  @Test
  public void testHitPairs() throws IOException {
    for (HitTest.HitPair hitPair : HitTest.HIT_PAIRS) {
      testHitPair_LessThan(hitPair);
    }
  }
  
  public void testHitPair_LessThan(HitTest.HitPair hitPair) throws IOException {
    LuceneServer.KattaHitQueue hitQueue = new LuceneServer.KattaHitQueue(1);
    boolean lessThan = hitQueue.lessThan(hitPair.hitA, hitPair.hitB);
    if (hitPair.compareValue <= 0) {
      assertFalse(lessThan);
    } else {
      assertTrue(lessThan);
    }
  }
}
