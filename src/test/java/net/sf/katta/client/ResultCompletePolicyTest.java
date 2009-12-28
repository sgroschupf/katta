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
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import net.sf.katta.AbstractTest;

import org.junit.Test;

/**
 * Test for {@link ResultCompletePolicy}.
 */
public class ResultCompletePolicyTest extends AbstractTest {

  @Test
  public void testCompleteShutdown() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(60000);
    assertEquals("Wait up to 60000 ms for complete results, then shut down.", rc.toString());
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "a");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "b");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "c");
    assertTrue(rc.waitTime(r) == -1);
    assertFalse(r.isClosed());
    //
    r = new ClientResult<String>(null, "a", "b", "c");
    rc = new ResultCompletePolicy<String>(60000, true);
    assertEquals("Wait up to 60000 ms for complete results, then shut down.", rc.toString());
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "a");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "b");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "c");
    assertTrue(rc.waitTime(r) == -1);
    assertFalse(r.isClosed());
  }

  @Test
  public void testCompleteNoShutdown() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(60000, false);
    assertEquals("Wait up to 60000 ms for complete results.", rc.toString());
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "a");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "b");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "c");
    assertTrue(rc.waitTime(r) == 0);
    assertFalse(r.isClosed());
  }

  @Test
  public void testCoverageNoShutdown() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(0, 60000, 0.5, false);
    assertEquals("Wait up to 0 ms for complete results, then 60000 ms for 0.5 coverage.", rc.toString());
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "a");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "b");
    assertTrue(rc.waitTime(r) == 0);
    assertFalse(r.isClosed());
    r.addResult("x", "c");
    assertTrue(rc.waitTime(r) == 0);
    assertFalse(r.isClosed());
  }

  @Test
  public void testCoverageShutdown() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(0, 60000, 0.5, true);
    assertEquals("Wait up to 0 ms for complete results, then 60000 ms for 0.5 coverage, then shut down.", rc.toString());
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "a");
    assertTrue(rc.waitTime(r) > 50000);
    r.addResult("x", "b");
    assertTrue(rc.waitTime(r) == -1);
    assertFalse(r.isClosed());
    r.addResult("x", "c");
    assertTrue(rc.waitTime(r) == -1);
    assertFalse(r.isClosed());
  }

  @Test
  public void testCompleteTiming() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(1000, true);
    long now = System.currentTimeMillis();
    long start = System.currentTimeMillis();
    long stop = start + 500;
    while (now < stop) {
      assertTrue(rc.waitTime(r) > 400);
      sleep(1);
      now = System.currentTimeMillis();
    }
    stop = start + 1500;
    while (now < stop) {
      sleep(stop - now);
      now = System.currentTimeMillis();
    }
    stop = start + 2000;
    while (now < stop) {
      assertTrue(rc.waitTime(r) == -1);
      sleep(1);
      now = System.currentTimeMillis();
    }
  }

  @Test
  public void testCoverageTiming1() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("foo", "a");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(500, 500, 0.5, false);
    assertEquals("Wait up to 500 ms for complete results, then 500 ms for 0.5 coverage.", rc.toString());
    long now = System.currentTimeMillis();
    long start = System.currentTimeMillis();
    long stop = start + 800;
    while (now < stop) {
      // Waiting for complete. Then wait for coverage.
      assertTrue(rc.waitTime(r) > 100);
      sleep(1);
      now = System.currentTimeMillis();
    }
    stop = start + 1500;
    while (now < stop) {
      sleep(stop - now);
      now = System.currentTimeMillis();
    }
    stop = start + 2000;
    while (now < stop) {
      // Expired.
      assertTrue(rc.waitTime(r) == 0);
      sleep(1);
      now = System.currentTimeMillis();
    }
  }

  @Test
  public void testCoverageTiming2() {
    ClientResult<String> r = new ClientResult<String>(null, "a", "b", "c");
    r.addResult("foo", "a", "b");
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(500, 500, 0.5, false);
    assertEquals("Wait up to 500 ms for complete results, then 500 ms for 0.5 coverage.", rc.toString());
    long now = System.currentTimeMillis();
    long start = System.currentTimeMillis();
    long stop = start + 250;
    while (now < stop) {
      // Wait for complete.
      assertTrue(rc.waitTime(r) > 300);
      sleep(1);
      now = System.currentTimeMillis();
    }
    stop = start + 600;
    while (now < stop) {
      sleep(stop - now);
      now = System.currentTimeMillis();
    }
    stop = start + 1200;
    while (now < stop) {
      // Coverage is good enough.
      assertTrue(rc.waitTime(r) == 0);
      sleep(1);
      now = System.currentTimeMillis();
    }
  }

  @Test
  public void testCoverage() {
    Set<String> shards = new HashSet<String>();
    for (int i = 0; i < 1000; i++) {
      shards.add("s" + i);
    }
    ClientResult<String> r = new ClientResult<String>(null, shards);
    ResultCompletePolicy<String> rc = new ResultCompletePolicy<String>(0, 60000, (879.0 / 1000.0), false);
    for (int i = 0; i < 1000; i++) {
      try {
        r.addResult("foo", "s" + i);
        if (i < 878) {
          assertTrue(rc.waitTime(r) > 30000);
        } else {
          assertTrue(rc.waitTime(r) == 0);
        }
      } catch (Error e) {
        System.err.println("i = " + i);
        throw e;
      }
    }
  }

  private void sleep(long msec) {
    long now = System.currentTimeMillis();
    long stop = now + msec;
    while (now < stop) {
      try {
        Thread.sleep(stop - now);
      } catch (InterruptedException e) {
        // proceed
      }
      now = System.currentTimeMillis();
    }
  }

}
