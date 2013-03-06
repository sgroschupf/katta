package net.sf.katta.integrationTest.lib.lucene;

import java.io.IOException;

import net.sf.katta.lib.lucene.Hit;
import net.sf.katta.lib.lucene.HitTest;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates that Hit's sorting behavior matches Lucene's sorting behavior
 */
public class Hit_MatchLuceneTest {
  @Test
  public void test() throws IOException {
	  for (HitTest.HitPair hitPair : HitTest.HIT_PAIRS) {
      testHitPair(hitPair);
	  }
  }
  
  public void testHitPair(HitTest.HitPair hitPair) throws IOException {
    Scorer scorer = mock(Scorer.class);
    when(scorer.score()).thenReturn(hitPair.hitA.getScore()).thenReturn(hitPair.hitB.getScore());
    
    TopScoreDocCollector docCollector = TopScoreDocCollector.create(1, false);
    docCollector.setScorer(scorer);
    docCollector.collect(hitPair.hitA.getDocId());
    docCollector.collect(hitPair.hitB.getDocId());
    
    TopDocs topDocs = docCollector.topDocs();
    Hit expectedHit;
    if (hitPair.compareValue == -1) {
      expectedHit = hitPair.hitA;
    } else {
      expectedHit = hitPair.hitB;
    }
    
    assertEquals(expectedHit.getScore(), topDocs.scoreDocs[0].score, 0);
    assertEquals(expectedHit.getDocId(), topDocs.scoreDocs[0].doc);
  }
}
