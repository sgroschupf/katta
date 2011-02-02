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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.sf.katta.AbstractTest;
import net.sf.katta.client.WorkQueue.INodeInteractionFactory;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link WorkQueue}.
 */
public class WorkQueueTest extends AbstractTest {

  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(WorkQueueTest.class);

  @Test
  public void testWorkQueue() throws Exception {
    TestShardManager sm = new TestShardManager();
    Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
    WorkQueue.resetInstanceCounter();
    for (int i = 0; i < 500; i++) {
      sm.reset();
      TestNodeInteractionFactory factory = new TestNodeInteractionFactory(10);
      WorkQueue<Integer> wq = new WorkQueue<Integer>(factory, sm, sm.allShards(), method, -1, 16);
      assertEquals(String.format("WorkQueue[TestServer.doSomething(16) (id=%d)]", i), wq.toString());
      Map<String, List<String>> plan = sm.createNode2ShardsMap(sm.allShards());
      for (String node : plan.keySet()) {
        wq.execute(node, plan, 1, 3);
      }
      ClientResult<Integer> r = wq.getResults(1000);
      int numNodes = plan.keySet().size();
      int numShards = sm.allShards().size();
      assertEquals(String.format("ClientResult: %d results, 0 errors, %d/%d shards (closed) (complete)", numNodes,
              numShards, numShards), r.toString());
      assertEquals(6, factory.getCalls().size());
      wq.shutdown();
    }
  }

  @Test
  public void testSubmitAfterShutdown() throws Exception {
    TestNodeInteractionFactory factory = new TestNodeInteractionFactory(10);
    TestShardManager sm = new TestShardManager();
    Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
    WorkQueue.resetInstanceCounter();
    WorkQueue<Integer> wq = new WorkQueue<Integer>(factory, sm, sm.allShards(), method, -1, 16);
    ClientResult<Integer> r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
    wq.shutdown();
    r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    Map<String, List<String>> plan = sm.createNode2ShardsMap(sm.allShards());
    for (String node : plan.keySet()) {
      wq.execute(node, plan, 1, 3);
    }
    r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    assertEquals(0, factory.getCalls().size());
  }

  @Test
  public void testSubmitAfterClose() throws Exception {
    TestNodeInteractionFactory factory = new TestNodeInteractionFactory(10);
    TestShardManager sm = new TestShardManager();
    Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
    WorkQueue.resetInstanceCounter();
    WorkQueue<Integer> wq = new WorkQueue<Integer>(factory, sm, sm.allShards(), method, -1, 16);
    ClientResult<Integer> r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
    r.close();
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    Map<String, List<String>> plan = sm.createNode2ShardsMap(sm.allShards());
    for (String node : plan.keySet()) {
      wq.execute(node, plan, 1, 3);
    }
    r = wq.getResults(0, false);
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    assertEquals(0, factory.getCalls().size());
  }

  @Test
  public void testGetResultTimeout() throws Exception {
    TestShardManager sm = new TestShardManager();
    Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
    WorkQueue.resetInstanceCounter();
    TestNodeInteractionFactory factory = new TestNodeInteractionFactory(10);
    factory.additionalSleepTime = 60000;
    WorkQueue<Integer> wq = new WorkQueue<Integer>(factory, sm, sm.allShards(), method, -1, 16);
    Map<String, List<String>> plan = sm.createNode2ShardsMap(sm.allShards());
    for (String node : plan.keySet()) {
      wq.execute(node, plan, 1, 3);
    }
    int numShards = sm.allShards().size();
    long slop = 20;
    // No delay
    long t1 = System.currentTimeMillis();
    ClientResult<Integer> r = wq.getResults(0, false);
    long t2 = System.currentTimeMillis();
    assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), r.toString());
    assertTrue(t2 - t1 < slop);
    // Short delay
    t1 = System.currentTimeMillis();
    r = wq.getResults(500, false);
    t2 = System.currentTimeMillis();
    assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), r.toString());
    assertTrue(t2 - t1 >= 500);
    assertTrue(t2 - t1 < 500 + slop);
    // Tiny delay.
    t1 = System.currentTimeMillis();
    r = wq.getResults(10, false);
    t2 = System.currentTimeMillis();
    assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards", numShards), r.toString());
    assertTrue(t2 - t1 >= 10);
    assertTrue(t2 - t1 < 10 + slop);
    // Stop soon.
    t1 = System.currentTimeMillis();
    r = wq.getResults(100, true);
    t2 = System.currentTimeMillis();
    assertEquals(String.format("ClientResult: 0 results, 0 errors, 0/%d shards (closed)", numShards), r.toString());
    assertTrue(t2 - t1 >= 100);
    assertTrue(t2 - t1 < 100 + slop);
    wq.shutdown();
  }

  // Does user calling close() wake up the work queue?
  @Test
  public void testUserCloseEvent() throws Exception {
    for (IResultPolicy<String> policy : new IResultPolicy[] { new ResultCompletePolicy<String>(4000),
            new ResultCompletePolicy<String>(4000, true), new ResultCompletePolicy<String>(4000, false),
            new ResultCompletePolicy<String>(50, 950, 0.99, true),
            new ResultCompletePolicy<String>(950, 50, 0.99, true),
            new ResultCompletePolicy<String>(50, 950, 0.99, false),
            new ResultCompletePolicy<String>(950, 50, 0.99, false) }) {
      INodeInteractionFactory<String> factory = nullFactory();
      TestShardManager sm = new TestShardManager();
      Method method = Object.class.getMethod("toString");
      WorkQueue.resetInstanceCounter();
      WorkQueue<String> wq = new WorkQueue<String>(factory, sm, sm.allShards(), method, -1);
      final ClientResult<String> result = wq.getResults(0, false);
      assertFalse(result.isClosed());
      sleep(10);
      /*
       * Simulate the user polling then eventually closing the result.
       */
      final long start = System.currentTimeMillis();
      new Thread(new Runnable() {
        public void run() {
          sleep(100);
          result.close();
        }
      }).start();
      /*
       * Now block on results.
       */
      ClientResult<String> result2 = wq.getResults(policy);
      long time = System.currentTimeMillis() - start;
      //
      if (time <= 50 || time >= 200) {
        System.err.println("Took " + time + ", expected 100. Policy = " + policy);
      }
      assertTrue(time > 50);
      assertTrue(time < 200);
      assertTrue(result2.isClosed());
      wq.shutdown();
    }
  }

  // Does IResultPolicy calling close() wake up the work queue?
  @Test(timeout = 10000)
  public void testPolicyCloseEvent() throws Exception {
    INodeInteractionFactory<String> factory = nullFactory();
    TestShardManager sm = new TestShardManager();
    Method method = Object.class.getMethod("toString");
    WorkQueue.resetInstanceCounter();
    IResultPolicy<String> policy = new IResultPolicy<String>() {
      private long now = System.currentTimeMillis();
      private long closeTime = now + 100;
      private long stopTime = now + 1000;

      public long waitTime(ClientResult<String> result) {
        final long innerNow = System.currentTimeMillis();
        if (innerNow >= closeTime) {
          result.close();
        }
        if (innerNow >= stopTime) {
          return 0;
        } else if (innerNow >= closeTime) {
          return stopTime - innerNow;
        } else {
          return closeTime - innerNow;
        }
      }
    };
    WorkQueue<String> wq = new WorkQueue<String>(factory, sm, sm.allShards(), method, -1);
    sleep(10);
    long startTime = System.currentTimeMillis();
    ClientResult<String> result = wq.getResults(policy);
    long time = System.currentTimeMillis() - startTime;
    assertTrue(result.isClosed());
    //
    if (time <= 50 || time >= 200) {
      System.err.println("Took " + time + ", expected 100. Policy = " + policy);
    }
    assertTrue(time > 50);
    assertTrue(time < 200);
    wq.shutdown();
  }

  @Test
  public void testPolling() throws Exception {
    TestShardManager sm = new TestShardManager(null, 80, 1);
    Method method = TestServer.class.getMethod("doSomething", Integer.TYPE);
    WorkQueue.resetInstanceCounter();
    TestNodeInteractionFactory factory = new TestNodeInteractionFactory(2500);
    WorkQueue<Integer> wq = new WorkQueue<Integer>(factory, sm, sm.allShards(), method, -1, 16);
    Map<String, List<String>> plan = sm.createNode2ShardsMap(sm.allShards());
    ClientResult<Integer> r = wq.getResults(0, false);
    System.out.println("Expected graph:");
    for (int len : new int[] { 0, 6, 12, 16, 23, 34, 40, 50, 51, 58, 64, 68, 76, 80 }) {
      bar(len);
    }
    System.out.println("Progress:");
    for (String node : plan.keySet()) {
      wq.execute(node, plan, 1, 3);
    }
    double coverage = 0.0;
    do {
      coverage = r.getShardCoverage();
      int len = (int) Math.round(coverage * 80);
      bar(len);
      if (coverage < 1.0) {
        sleep(200);
      }
    } while (coverage < 1.0);
    System.out.println("Done.");
    wq.shutdown();
  }

  private void bar(int len) {
    StringBuilder sb = new StringBuilder();
    sb.append('|');
    for (int i = 0; i < 80; i++) {
      sb.append(i < len ? '#' : ' ');
    }
    sb.append('|');
    System.out.println(sb);
  }

  public interface ProxyProvider {
    public VersionedProtocol getProxy(String node);
  }

  public static class TestShardManager implements INodeProxyManager {

    private int numNodes;
    private int replication;
    private List<String> allNodes;
    private Set<String> allShards;
    private Map<String, List<String>> shardMap;
    private INodeSelectionPolicy _selectionPolicy;
    private ProxyProvider proxyProvider;
    private boolean shardMapsFail = false;

    public TestShardManager() {
      this(null, 8, 3);
    }

    public TestShardManager(ProxyProvider proxyProvider, int numNodes, int replication) {
      this.proxyProvider = proxyProvider;
      this.numNodes = numNodes;
      this.replication = replication;
      reset();
    }

    public void reset() {
      // Nodes n1, n2, n3...
      String[] nodes = new String[numNodes];
      for (int i = 0; i < numNodes; i++) {
        nodes[i] = "n" + (i + 1);
      }
      allNodes = Arrays.asList(nodes);
      // Shards s1, s3, s3... (same # as nodes)
      String[] shards = new String[numNodes];
      for (int i = 0; i < numNodes; i++) {
        shards[i] = "s" + (i + 1);
      }
      allShards = new HashSet<String>(Arrays.asList(shards));
      // Node i has shards i, i+1, i+2... depending on replication level.
      shardMap = new HashMap<String, List<String>>();
      for (int i = 0; i < numNodes; i++) {
        List<String> shardList = new ArrayList<String>();
        for (int j = 0; j < replication; j++) {
          shardList.add(shards[(i + j) % numNodes]);
        }
        shardMap.put(nodes[i], shardList);
      }
      // Compute reverse map.
      _selectionPolicy = new DefaultNodeSelectionPolicy();
      for (int i = 0; i < numNodes; i++) {
        String thisShard = shards[i];
        List<String> nodeList = new ArrayList<String>();
        for (int j = 0; j < numNodes; j++) {
          if (shardMap.get(nodes[j]).contains(thisShard)) {
            nodeList.add(nodes[j]);
          }
        }
        _selectionPolicy.update(thisShard, nodeList);
      }
      shardMapsFail = false;
    }

    public void setShardMapsFail(boolean shardMapsFail) {
      this.shardMapsFail = shardMapsFail;
    }

    public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
      if (shardMapsFail) {
        throw new ShardAccessException("Test error");
      }
      return Collections.unmodifiableMap(_selectionPolicy.createNode2ShardsMap(shards));
    }

    public VersionedProtocol getProxy(String node, boolean establishIfNotExists) {
      return proxyProvider != null ? proxyProvider.getProxy(node) : null;
    }

    public void reportNodeCommunicationFailure(String node, Throwable t) {
      _selectionPolicy.removeNode(node);
    }

    public List<String> allNodes() {
      return Collections.unmodifiableList(allNodes);
    }

    public Set<String> allShards() {
      return Collections.unmodifiableSet(allShards);
    }

    public Map<String, List<String>> getMap() {
      return Collections.unmodifiableMap(shardMap);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void reportNodeCommunicationSuccess(String node) {

    }

  }

  public static class TestNodeInteractionFactory implements INodeInteractionFactory<Integer> {

    public class Entry {
      public String node;
      public Method method;
      public Object[] args;

      public Entry(String node, Method method, Object[] args) {
        this.node = node;
        this.method = method;
        this.args = args;
      }

      @Override
      public String toString() {
        return node + ":" + method.getName() + ":" + Arrays.asList(args).toString();
      }
    }

    public List<Entry> calls = new ArrayList<Entry>();
    public int maxSleep;
    public long additionalSleepTime = 0; // TODO combine sleeps

    public TestNodeInteractionFactory(int maxSleep) {
      this.maxSleep = maxSleep;
    }

    public Runnable createInteraction(Method method, final Object[] args, int shardArrayParamIndex, final String node,
            Map<String, List<String>> nodeShardMap, int tryCount, int maxTryCount, INodeProxyManager shardManager,
            INodeExecutor nodeExecutor, final IResultReceiver<Integer> results) {
      calls.add(new Entry(node, method, args));
      final long additionalSleepTime2 = additionalSleepTime;
      final TestServer server = new TestServer(maxSleep);
      final List<String> shards = nodeShardMap.get(node);
      return new Runnable() {
        public void run() {
          if (additionalSleepTime2 > 0) {
            sleep(additionalSleepTime2);
          }
          int n = ((Integer) args[0]).intValue();
          int r = server.doSomething(n);
          // System.out.printf("Test interaction, node=%s, f(%d)=%d, shards=%s\n",
          // node, n, r, shards);
          results.addResult(r, shards);
        }
      };
    }

    public List<Entry> getCalls() {
      return calls;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      String sep = "";
      for (Entry entry : calls) {
        sb.append(sep);
        sb.append(entry.toString());
        sep = ", ";
      }
      return sb.toString();
    }

  }

  private static class TestServer {
    private static Random rand = new Random("testserver".hashCode());
    private int maxSleep;

    public TestServer(int maxSleep) {
      this.maxSleep = maxSleep;
    }

    public int doSomething(int n) {
      long msec = rand.nextInt(maxSleep);
      sleep(msec);
      return n * 2;
    }
  }

  /** Returns an interaction factory that ignores all calls and does nothing. */
  public static <T> INodeInteractionFactory<T> nullFactory() {
    return new INodeInteractionFactory<T>() {
      public Runnable createInteraction(Method method, Object[] args, int shardArrayParamIndex, String node,
              Map<String, List<String>> nodeShardMap, int tryCount, int maxTryCount, INodeProxyManager shardManager,
              INodeExecutor nodeExecutor, IResultReceiver<T> results) {
        return null;
      }
    };
  }

  protected static void sleep(long msec) {
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
