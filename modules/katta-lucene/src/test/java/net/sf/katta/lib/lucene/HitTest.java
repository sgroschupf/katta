package net.sf.katta.lib.lucene;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import static org.junit.Assert.assertEquals;

public class HitTest {
  public static class HitPair {
    public final Hit hitA;
    public final Hit hitB;
    public final int compareValue;
    
    public HitPair(Hit hitA, Hit hitB, int compareValue) {
      this.hitA = hitA;
      this.hitB = hitB;
      this.compareValue = compareValue;
    }
  }
  
  public static List<HitPair> HIT_PAIRS = ImmutableList.<HitTest.HitPair>of(
      new HitPair(new Hit("shard1", "node1", 1.0f, 1, null), new Hit("shard1", "node1", 0.8f, 1, null), -1),
      new HitPair(new Hit("shard1", "node1", 0.8f, 1, null), new Hit("shard1", "node1", 1.0f, 1, null),  1),
      new HitPair(new Hit("shard1", "node1", 1.0f, 1, null), new Hit("shard1", "node1", 1.0f, 2, null), -1),
      new HitPair(new Hit("shard1", "node1", 1.0f, 2, null), new Hit("shard1", "node1", 1.0f, 1, null),  1),
      new HitPair(new Hit("shard2", "node1", 1.0f, 1, null), new Hit("shard1", "node1", 1.0f, 1, null), -1),
      new HitPair(new Hit("shard1", "node1", 1.0f, 1, null), new Hit("shard1", "node1", 1.0f, 1, null),  0),
      new HitPair(new Hit("shard1", "node1", 1.0f, 1, null), new Hit("shard2", "node1", 1.0f, 1, null),  1)
  );
  
  @Test
  public void testCompareTo() {
    for (HitPair hitPair: HIT_PAIRS) {
      assertEquals(String.format("%s.compareTo(%s): %d", hitPair.hitA, hitPair.hitB, hitPair.compareValue),
          hitPair.compareValue, hitPair.hitA.compareTo(hitPair.hitB));
    }
  }
}
