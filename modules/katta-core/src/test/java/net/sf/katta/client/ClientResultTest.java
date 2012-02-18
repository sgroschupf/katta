/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.AbstractTest;
import net.sf.katta.client.ClientResult.IClosedListener;

import org.junit.Test;

/**
 * Test for {@link ClientResult}.
 */
public class ClientResultTest extends AbstractTest {

  @Test
  public void testToStringResults() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
    r.addResult("x", "a");
    assertEquals("ClientResult: 1 results, 0 errors, 1/3 shards", r.toString());
    r.addResult("x", "b");
    assertEquals("ClientResult: 2 results, 0 errors, 2/3 shards", r.toString());
    r.addResult(null, "c");
    assertEquals("ClientResult: 2 results, 0 errors, 3/3 shards (complete)", r.toString());
    r.close();
    assertEquals("ClientResult: 2 results, 0 errors, 3/3 shards (closed) (complete)", r.toString());
  }

  @Test
  public void testToStringErrors() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
    r.addError(new Throwable(""), "a");
    assertEquals("ClientResult: 0 results, 1 errors, 1/3 shards", r.toString());
    r.addError(new Exception(""), "b");
    assertEquals("ClientResult: 0 results, 2 errors, 2/3 shards", r.toString());
    r.addError(null, "c");
    assertEquals("ClientResult: 0 results, 2 errors, 3/3 shards (complete)", r.toString());
  }

  @Test
  public void testToStringMixed() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards", r.toString());
    r.addResult("x", "a");
    assertEquals("ClientResult: 1 results, 0 errors, 1/3 shards", r.toString());
    r.addResult("x", "b");
    assertEquals("ClientResult: 2 results, 0 errors, 2/3 shards", r.toString());
    r.addError(new Exception(), "c");
    assertEquals("ClientResult: 2 results, 1 errors, 3/3 shards (complete)", r.toString());
  }

  @Test
  public void testResults() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c", "d");
    assertEquals("[]", r.getResults().toString());
    r.addResult("r1", "a");
    assertEquals("[r1]", r.getResults().toString());
    r.addError(new Exception("foo"), "b");
    assertEquals("[r1]", r.getResults().toString());
    r.addResult("r2", "c");
    assertEquals(8, r.getResults().toString().length());
    assertTrue(r.getResults().toString().indexOf("r1") > 0);
    assertTrue(r.getResults().toString().indexOf("r2") > 0);
    r.addResult(null, "c");
    assertEquals(8, r.getResults().toString().length());
    assertTrue(r.getResults().toString().indexOf("r1") > 0);
    assertTrue(r.getResults().toString().indexOf("r2") > 0);
  }

  @Test
  public void testDuplicateResults() {
    ClientResult<Integer> r = new ClientResult<Integer>(null, "a", "b", "c");
    r.addResult(5, "a");
    r.addResult(5, "b");
    r.addResult(5, "c");
    assertEquals(3, r.getResults().size());
    assertEquals(3, r.entrySet().size());
    r.addResult(5, "a");
    assertEquals(4, r.getResults().size());
    assertEquals(4, r.entrySet().size());
  }

  @Test
  public void testDuplicateErrors() {
    ClientResult<Integer> r = new ClientResult<Integer>(null, "a", "b", "c");
    Throwable t = new Throwable();
    r.addError(t, "a");
    r.addError(t, "b");
    r.addError(t, "c");
    assertEquals(3, r.getErrors().size());
    assertEquals(3, r.entrySet().size());
    r.addError(t, "a");
    assertEquals(4, r.getErrors().size());
    assertEquals(4, r.entrySet().size());
  }

  @Test
  public void testErrors() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c", "d");
    assertEquals("[]", r.getErrors().toString());
    assertNull(r.getError());
    assertFalse(r.isError());
    r.addResult("r1", "a");
    assertEquals("[]", r.getErrors().toString());
    assertFalse(r.isError());
    assertNull(r.getError());
    r.addError(new OutOfMemoryError("foo"), "b");
    assertEquals("[java.lang.OutOfMemoryError: foo]", r.getErrors().toString());
    assertEquals("java.lang.OutOfMemoryError: foo", r.getError().toString());
    assertTrue(r.isError());
    r.addError(new NullPointerException(), "c");
    assertEquals(65, r.getErrors().toString().length());
    assertTrue(r.getErrors().toString().indexOf("java.lang.OutOfMemoryError: foo") > 0);
    assertTrue(r.getErrors().toString().indexOf("java.lang.NullPointerException") > 0);
    assertTrue(r.getError() instanceof OutOfMemoryError || r.getError() instanceof NullPointerException);
    assertTrue(r.isError());
  }

  @Test
  public void testNoShards() {
    try {
      new ClientResult<String>((IClosedListener) null, (Collection<String>) null);
      fail("Should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // Good.
    }
    try {
      new ClientResult<String>((IClosedListener) null, new ArrayList<String>());
      fail("Should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // Good.
    }
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("foo", (Collection<String>) null);
    assertTrue(r.getSeenShards().isEmpty());
    assertTrue(r.getResults().isEmpty());
    r.addResult("foo", new ArrayList<String>());
    assertTrue(r.getSeenShards().isEmpty());
    assertTrue(r.getResults().isEmpty());
    r.addError(new Exception("foo"), (Collection<String>) null);
    assertTrue(r.getSeenShards().isEmpty());
    assertTrue(r.getErrors().isEmpty());
    r.addError(new Exception("foo"), new ArrayList<String>());
    assertTrue(r.getSeenShards().isEmpty());
    assertTrue(r.getErrors().isEmpty());
  }

  @Test
  public void testNulls() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult(null, "a");
    assertEquals(1, r.getSeenShards().size());
    assertTrue(r.getSeenShards().contains("a"));
    assertTrue(r.getResults().isEmpty());
    assertTrue(r.getErrors().isEmpty());
    assertFalse(r.isError());
    assertEquals(0.3333, r.getShardCoverage(), 0.001);
    assertEquals(1, r.getArrivalTimes().size());
    assertTrue(r.getArrivalTimes().get(0).toString().startsWith("null from [a] at "));
    sleep(3);
    r.addError(null, "b");
    assertEquals(2, r.getSeenShards().size());
    assertTrue(r.getSeenShards().contains("b"));
    assertTrue(r.getResults().isEmpty());
    assertTrue(r.getErrors().isEmpty());
    assertFalse(r.isError());
    assertEquals(0.6666, r.getShardCoverage(), 0.001);
    assertEquals(2, r.getArrivalTimes().size());
    assertTrue(r.getArrivalTimes().get(1).toString().startsWith("null from [b] at "));
    sleep(3);
    r.addResult(null, "c");
    assertEquals(3, r.getSeenShards().size());
    assertTrue(r.getSeenShards().contains("c"));
    assertTrue(r.getResults().isEmpty());
    assertTrue(r.getErrors().isEmpty());
    assertFalse(r.isError());
    assertEquals(1.0, r.getShardCoverage(), 0.001);
    assertTrue(r.getArrivalTimes().get(2).toString().startsWith("null from [c] at "));
    assertEquals(3, r.getArrivalTimes().size());
    assertTrue(r.isComplete());
    assertTrue(r.isOK());
  }

  protected static class ToStringFails {
    @Override
    public String toString() {
      throw new RuntimeException("err");
    }
  }

  @Test
  public void testEntryBadResult() {
    ClientResult<ToStringFails> r = new ClientResult<ToStringFails>(null, "a", "b", "c");
    r.addResult(new ToStringFails(), "c");
    assertTrue(r.entrySet().iterator().next().toString().startsWith("(toString() err) from [c] at "));
    r = new ClientResult<ToStringFails>(null, "a", "b", "c");
    r.addResult(null, "c");
    assertTrue(r.entrySet().iterator().next().toString().startsWith("null from [c] at "));
    r = new ClientResult<ToStringFails>(null, "a", "b", "c");
    r.addError(null, "c");
    assertTrue(r.entrySet().iterator().next().toString().startsWith("null from [c] at "));
  }

  @Test
  public void testMissingAndSeenShardsSingle() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    List<String> missing = new ArrayList<String>(r.getMissingShards());
    Collections.sort(missing);
    assertEquals("[a, b, c]", missing.toString());
    assertTrue(r.getSeenShards().isEmpty());
    //
    r.addResult("x", "b");
    missing = new ArrayList<String>(r.getMissingShards());
    Collections.sort(missing);
    assertEquals("[a, c]", missing.toString());
    List<String> seen = new ArrayList<String>(r.getSeenShards());
    Collections.sort(seen);
    assertEquals("[b]", seen.toString());
    //
    r.addError(new Exception(""), "a");
    missing = new ArrayList<String>(r.getMissingShards());
    Collections.sort(missing);
    assertEquals("[c]", missing.toString());
    seen = new ArrayList<String>(r.getSeenShards());
    Collections.sort(seen);
    assertEquals("[a, b]", seen.toString());
    //
    r.addResult("x", "c");
    assertTrue(r.getMissingShards().isEmpty());
    seen = new ArrayList<String>(r.getSeenShards());
    Collections.sort(seen);
    assertEquals("[a, b, c]", seen.toString());
  }

  @Test
  public void testMissingAndSeenShardsMulti() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("x", "a", "b");
    List<String> missing = new ArrayList<String>(r.getMissingShards());
    Collections.sort(missing);
    assertEquals("[c]", missing.toString());
    List<String> seen = new ArrayList<String>(r.getSeenShards());
    Collections.sort(seen);
    assertEquals("[a, b]", seen.toString());
    //
    r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("x", "a", "b", "c");
    assertTrue(r.getMissingShards().isEmpty());
    missing = new ArrayList<String>(r.getMissingShards());
    Collections.sort(missing);
    seen = new ArrayList<String>(r.getSeenShards());
    Collections.sort(seen);
    assertEquals("[a, b, c]", seen.toString());
  }

  @Test
  public void testUnknownShards() {
    // This should not happen normally.
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertEquals(3, r.getMissingShards().size());
    assertEquals(0, r.getSeenShards().size());
    assertEquals(0, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "x");
    assertEquals(3, r.getMissingShards().size());
    assertEquals(1, r.getSeenShards().size());
    assertEquals(1, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "y");
    assertEquals(3, r.getMissingShards().size());
    assertEquals(2, r.getSeenShards().size());
    assertEquals(2, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "z");
    assertEquals(3, r.getMissingShards().size());
    assertEquals(3, r.getSeenShards().size());
    assertEquals(3, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "a", "b", "c");
    assertEquals(0, r.getMissingShards().size());
    assertEquals(6, r.getSeenShards().size());
    assertEquals(4, r.entrySet().size());
    assertTrue(r.isComplete());
  }

  @Test
  public void testDuplicateShards() {
    // This should not happen normally.
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertEquals(3, r.getMissingShards().size());
    assertEquals(0, r.getSeenShards().size());
    assertEquals(0, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "a", "b");
    assertEquals(1, r.getMissingShards().size());
    assertEquals(2, r.getSeenShards().size());
    assertEquals(1, r.entrySet().size());
    assertFalse(r.isComplete());
    r.addResult("foo", "b", "c");
    assertEquals(0, r.getMissingShards().size());
    assertEquals(3, r.getSeenShards().size());
    assertEquals(2, r.entrySet().size());
    assertTrue(r.isComplete());
    r.addResult("foo", "c", "a");
    assertEquals(0, r.getMissingShards().size());
    assertEquals(3, r.getSeenShards().size());
    assertEquals(3, r.entrySet().size());
    assertTrue(r.isComplete());
  }

  @Test
  public void testClosingCallback() {
    final AtomicInteger count = new AtomicInteger(0);
    ClientResult<String> r = new ClientResult<String>(new IClosedListener() {
      public void clientResultClosed() {
        count.incrementAndGet();
      }
    }, "a", "b", "c");
    assertEquals(0, count.get());
    r.close();
    assertEquals(1, count.get());
    r.close();
    assertEquals(1, count.get());
    // Test no listener.
    r = new ClientResult<String>(null, "shard");
    assertFalse(r.isClosed());
    r.close();
    assertTrue(r.isClosed());
  }

  @Test
  public void testClosed() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.close();
    r.addResult("r1", "a");
    r.addError(new Exception(), "b", "c");
    assertEquals("ClientResult: 0 results, 0 errors, 0/3 shards (closed)", r.toString());
  }

  @Test
  public void testArrivalTimes() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("r1", "a");
    sleep(3);
    Throwable t = new Throwable();
    r.addError(t, "b");
    sleep(3);
    r.addResult("r2", "c");
    List<ClientResult<String>.Entry> entries = r.getArrivalTimes();
    assertNotNull(entries);
    assertEquals(3, entries.size());
    ClientResult<String>.Entry e1 = entries.get(0);
    ClientResult<String>.Entry e2 = entries.get(1);
    ClientResult<String>.Entry e3 = entries.get(2);
    assertEquals("r1", e1.result);
    assertEquals(t, e2.error);
    assertEquals("r2", e3.result);
    assertTrue(e2.time > e1.time);
    assertTrue(e3.time > e2.time);
  }

  @Test
  public void testArrivalTimesSorting() {
    String result = "foo";
    Throwable error = new Exception("bar");
    Set<String> shardA = new HashSet<String>();
    shardA.add("a");
    Set<String> shardB = new HashSet<String>();
    shardB.add("b");
    for (int i = 0; i < 10000; i++) {
      ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
      if (i % 1 == 0) {
        r.addResult(result, shardA);
        r.addError(error, shardB);
      } else {
        r.addError(error, shardB);
        r.addResult(result, shardA);
      }
      List<ClientResult<String>.Entry> times = r.getArrivalTimes();
      assertEquals(2, times.size());
      assertEquals("foo", times.get(0).result);
      assertNull(times.get(0).error);
      assertEquals("[a]", times.get(0).shards.toString());
      assertNull(times.get(1).result);
      assertEquals("java.lang.Exception: bar", times.get(1).error.toString());
      assertEquals("[b]", times.get(1).shards.toString());
    }
  }

  @Test
  public void testMultithreaded() throws InterruptedException {
    Set<String> shards = new HashSet<String>();
    final int size = 1000;
    for (int i = 0; i < size; i++) {
      shards.add("s" + i);
    }
    final ClientResult<Integer> r = new ClientResult<Integer>(null, shards);
    Random rand = new Random("testMultithreaded".hashCode());
    ExecutorService executor = Executors.newFixedThreadPool(15);
    int total = 0;
    for (int i = 0; i < size; i++) {
      final String shard = "s" + i;
      final int result = i;
      total += result;
      final long delay = rand.nextInt(50);
      executor.submit(new Runnable() {
        public void run() {
          sleep(delay);
          r.addResult(result, shard);
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(3, TimeUnit.MINUTES);
    r.close();
    //
    assertEquals(String
            .format("ClientResult: %s results, 0 errors, %d/%d shards (closed) (complete)", size, size, size), r
            .toString());
    int resultTotal = 0;
    for (ClientResult<Integer>.Entry e : r) {
      resultTotal += e.result;
    }
    assertEquals(total, resultTotal);
  }

  @Test
  public void testIteratorEmpty() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    assertFalse(r.iterator().hasNext());
  }

  @Test
  public void testIteratorWhileAdding() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("r1", "a");
    r.addResult("r2", "b");
    Iterator<ClientResult<String>.Entry> i = r.iterator();
    r.addResult("r3", "c");
    assertTrue(i.hasNext());
    ClientResult<String>.Entry e1 = i.next();
    assertTrue(e1.result.equals("r1") || e1.result.equals("r2"));
    assertTrue(e1.shards.size() == 1);
    assertTrue(e1.shards.iterator().next().equals("a") || e1.shards.iterator().next().equals("b"));
    ClientResult<String>.Entry e2 = i.next();
    assertTrue(e2.result.equals("r1") || e2.result.equals("r2"));
    assertTrue(e2.shards.size() == 1);
    assertTrue(e2.shards.iterator().next().equals("a") || e2.shards.iterator().next().equals("b"));
    assertFalse(e1.result.equals(e2.result));
    assertFalse(e1.shards.iterator().next().equals(e2.shards.iterator().next()));
    assertFalse(i.hasNext());
  }

  @Test
  public void testReadOnly() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    checkReadOnly(r);
    r.addResult("r1", "a");
    checkReadOnly(r);
    r.addError(new Exception(), "b");
    checkReadOnly(r);
    r.addResult("r3", "c");
    checkReadOnly(r);
    r.close();
    checkReadOnly(r);
  }

  private void checkReadOnly(ClientResult<String> r) {
    checkCollectionReadOnly(r.getAllShards());
    checkCollectionReadOnly(r.entrySet());
    checkCollectionReadOnly(r.getResults());
    checkCollectionReadOnly(r.getErrors());
    checkCollectionReadOnly(r.getSeenShards());
    try {
      r.iterator().remove();
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  private void checkCollectionReadOnly(Collection<?> s) {
    try {
      s.remove(0);
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      s.clear();
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    if (!s.isEmpty()) {
      try {
        s.remove(s.iterator().next());
        fail("Should be read only");
      } catch (UnsupportedOperationException e) {
        // expected
      }
    }
    try {
      s.removeAll(new ArrayList<ClientResult<String>.Entry>());
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      s.retainAll(new ArrayList<ClientResult<String>.Entry>());
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      s.add(null);
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      s.addAll(new ArrayList());
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      s.iterator().remove();
      fail("Should be read only");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  @Test
  public void testCoverage() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c", "d", "e");
    assertEquals(0.0, r.getShardCoverage(), 0.000001);
    assertFalse(r.isComplete());
    assertFalse(r.isOK());
    r.addResult("r1", "a");
    assertEquals(0.2, r.getShardCoverage(), 0.000001);
    assertFalse(r.isComplete());
    assertFalse(r.isOK());
    r.addResult("r1", "b");
    assertEquals(0.4, r.getShardCoverage(), 0.000001);
    assertFalse(r.isComplete());
    assertFalse(r.isOK());
    r.addResult("r1", "c", "d");
    assertEquals(0.8, r.getShardCoverage(), 0.000001);
    assertFalse(r.isComplete());
    assertFalse(r.isOK());
    r.addResult("r1", "e");
    assertTrue(r.isComplete());
    assertTrue(r.isOK());
    assertEquals(1.0, r.getShardCoverage(), 0.000001);
  }

  @Test
  public void testStartTime() {
    ClientResult<String> r1 = new ClientResult<String>(null, "a", "b", "c");
    sleep(10);
    ClientResult<String> r2 = new ClientResult<String>(null, "a", "b", "c");
    assertTrue(r2.getStartTime() - r1.getStartTime() >= 10);
  }

  protected void sleep(long msec) {
    long now = System.currentTimeMillis();
    long waitUntil = now + msec;
    while (now < waitUntil) {
      long remainingTime = waitUntil - now;
      try {
        Thread.sleep(remainingTime);
      } catch (InterruptedException e) {
        // ignore and continue waiting
      }
      now = System.currentTimeMillis();
    }
  }

}
