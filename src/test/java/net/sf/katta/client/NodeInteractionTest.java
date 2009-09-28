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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.testutil.ExtendedTestCase;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Test for {@link NodeInteraction}.
 */
public class NodeInteractionTest extends ExtendedTestCase {

  private TestProxyProvider pp;
  private WorkQueueTest.TestShardManager sm;
  private TestNodeExecutor ne;
  private Map<String, List<String>> map;

  @Override
  protected void onSetUp() throws Exception {
    pp = new TestProxyProvider();
    sm = new WorkQueueTest.TestShardManager(pp, 8, 3);
    ne = new TestNodeExecutor();
    map = sm.createNode2ShardsMap(sm.allShards());
  }

  public void testNormalCall() throws Exception {
    Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
    Object[] args = new Object[] { "foo", null };
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
    assertEquals("n1:foo:[s2, s1, s3]", pp.toString());
    assertEquals("", ne.toString());
  }

  public void testNormalCallNoShardsParam() throws Exception {
    Method method = ITestServer.class.getMethod("testMethodNoShards", String.class);
    Object[] args = new Object[] { "foo" };
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    Runnable ni = new NodeInteraction<String>(method, args, -1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call testMethodNoShards on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
    assertEquals("n1:foo:null", pp.toString());
    assertEquals("", ne.toString());
  }

  public void testRetries() throws Exception {
    Method method = ITestServer.class.getMethod("fails", String.class, String[].class);
    Object[] args = new Object[] { "foo", null };
    /*
     * First try to call node n1 with shards s1, s2, s3. TryCount = 1. Node
     * fails.
     */
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    assertEquals(3, map.get("n1").size());
    assertTrue(map.get("n1").contains("s1"));
    assertTrue(map.get("n1").contains("s2"));
    assertTrue(map.get("n1").contains("s3"));
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call fails on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
    assertEquals("n1:null:null", pp.toString());
    assertEquals("n3:2:{n3=[s3], n8=[s2, s1]}, n8:2:{n3=[s3], n8=[s2, s1]}", ne.toString());
    List<NodeInteractionTest.TestNodeExecutor.Call> retriesA = ne.calls;
    /*
     * Now simulate running the 2 retries. TryCount = 2. Node n3 with shard s3.
     * Node fails.
     */
    ne = new TestNodeExecutor();
    NodeInteractionTest.TestNodeExecutor.Call call = retriesA.get(0);
    assertEquals("n3", call.node);
    assertEquals(1, call.nodeShardMap.get(call.node).size());
    assertTrue(call.nodeShardMap.get(call.node).contains("s3"));
    r = new ClientResult<String>(null, sm.allShards());
    ni = new NodeInteraction<String>(method, args, 1, call.node, call.nodeShardMap, 2, sm, ne, r);
    ni.run();
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
    assertEquals("n1:null:null, n3:null:null", pp.toString());
    assertEquals("n2:3:{n2=[s3]}", ne.toString());
    NodeInteractionTest.TestNodeExecutor.Call retryB1 = ne.calls.get(0);
    /*
     * Second retry. TryCount = 2. Node n8 with shards s1, s2. Node fails.
     */
    ne = new TestNodeExecutor();
    call = retriesA.get(1);
    assertEquals("n8", call.node);
    assertEquals(2, call.nodeShardMap.get(call.node).size());
    assertTrue(call.nodeShardMap.get(call.node).contains("s1"));
    assertTrue(call.nodeShardMap.get(call.node).contains("s2"));
    r = new ClientResult<String>(null, sm.allShards());
    ni = new NodeInteraction<String>(method, args, 1, call.node, call.nodeShardMap, 2, sm, ne, r);
    ni.run();
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards", r.toString());
    assertEquals("n1:null:null, n3:null:null, n8:null:null", pp.toString());
    assertEquals("n2:3:{n2=[s2], n7=[s1]}, n7:3:{n2=[s2], n7=[s1]}", ne.toString());
    List<NodeInteractionTest.TestNodeExecutor.Call> retriesB2 = ne.calls;
    /*
     * Third round of retries. TryCount = 3. No further retry attempts. Node n2
     * with shard s3. Node fails.
     */
    ne = new TestNodeExecutor();
    assertEquals("n2", retryB1.node);
    assertEquals(1, retryB1.nodeShardMap.get(retryB1.node).size());
    assertTrue(retryB1.nodeShardMap.get(retryB1.node).contains("s3"));
    r = new ClientResult<String>(null, sm.allShards());
    ni = new NodeInteraction<String>(method, args, 1, retryB1.node, retryB1.nodeShardMap, 3, sm, ne, r);
    ni.run();
    assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
    assertEquals("n1:null:null, n2:null:null, n3:null:null, n8:null:null", pp.toString());
    assertEquals("", ne.toString());
    /*
     * Node n2 with shard s2. TryCount = 3. Node fails.
     */
    ne = new TestNodeExecutor();
    call = retriesB2.get(0);
    assertEquals("n2", call.node);
    assertEquals(1, call.nodeShardMap.get(call.node).size());
    assertTrue(call.nodeShardMap.get(call.node).contains("s2"));
    r = new ClientResult<String>(null, sm.allShards());
    ni = new NodeInteraction<String>(method, args, 1, call.node, call.nodeShardMap, 3, sm, ne, r);
    ni.run();
    assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
    assertEquals("n1:null:null, n2:null:null, n3:null:null, n8:null:null", pp.toString());
    assertEquals("", ne.toString());
    /*
     * Node n7 with shard s1. TryCount = 3. Node fails.
     */
    ne = new TestNodeExecutor();
    call = retriesB2.get(1);
    assertEquals("n7", call.node);
    assertEquals(1, call.nodeShardMap.get(call.node).size());
    assertTrue(call.nodeShardMap.get(call.node).contains("s1"));
    r = new ClientResult<String>(null, sm.allShards());
    ni = new NodeInteraction<String>(method, args, 1, call.node, call.nodeShardMap, 3, sm, ne, r);
    ni.run();
    assertEquals("ClientResult: 0 results, 1 errors, 1/8 shards", r.toString());
    assertEquals("n1:null:null, n2:null:null, n3:null:null, n7:null:null, n8:null:null", pp.toString());
    assertEquals("", ne.toString());
  }

  public void testRetriesUserClosedResult() throws Exception {
    Method method = TestServer.class.getMethod("fails", String.class, String[].class);
    Object[] args = new Object[] { "foo", null };
    /*
     * Close the result object. Then try to call node n1 with shards s1, s2, s3.
     * TryCount = 1. Node fails. No retries should be attempted.
     */
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    r.close();
    assertEquals(3, map.get("n1").size());
    assertTrue(map.get("n1").contains("s1"));
    assertTrue(map.get("n1").contains("s2"));
    assertTrue(map.get("n1").contains("s3"));
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call fails on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 0 results, 0 errors, 0/8 shards (closed)", r.toString());
    assertEquals("n1:null:null", pp.toString());
    assertEquals("", ne.toString());
  }

  public void testRetriesPolicyFailure() throws Exception {
    sm.setShardMapsFail(true);
    Method method = ITestServer.class.getMethod("fails", String.class, String[].class);
    Object[] args = new Object[] { "foo", null };
    /*
     * Try to call node n1 with shards s1, s2, s3. TryCount = 1. Node fails.
     * When attempting to create retry node shard map, policy will throw an
     * exception. Give up on retries and log error.
     */
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    assertEquals(3, map.get("n1").size());
    assertTrue(map.get("n1").contains("s1"));
    assertTrue(map.get("n1").contains("s2"));
    assertTrue(map.get("n1").contains("s3"));
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call fails on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 0 results, 1 errors, 3/8 shards", r.toString());
    assertEquals("net.sf.katta.client.ShardAccessException: Shard 'Test error' is currently not reachable", r
            .getErrors().iterator().next().toString());
    assertEquals("n1:null:null", pp.toString());
    assertEquals("", ne.toString());
  }

  public void testNoProxy() throws Exception {
    pp.returnNullFor("n1");
    Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
    Object[] args = new Object[] { "foo", null };
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
    ni.run();
    assertEquals("ClientResult: 0 results, 1 errors, 3/8 shards", r.toString());
    assertEquals("", pp.toString());
    assertEquals("", ne.toString());
    assertEquals("[net.sf.katta.util.KattaException: No proxy for node: n1]", r.getErrors().toString());
  }

  public void testDefensiveArgCopy() throws Exception {
    Method method = ITestServer.class.getMethod("testMethod", String.class, String[].class);
    Object[] args = new Object[] { "OK", null };
    ClientResult<String> r = new ClientResult<String>(null, sm.allShards());
    Runnable ni = new NodeInteraction<String>(method, args, 1, "n1", map, 1, sm, ne, r);
    assertEquals("NodeInteraction: call testMethod on n1", ni.toString());
    args[0] = "FAIL";
    ni.run();
    assertEquals("ClientResult: 1 results, 0 errors, 3/8 shards", r.toString());
    assertEquals("n1:OK:[s2, s1, s3]", pp.toString());
    assertEquals("", ne.toString());
  }

  private static class TestNodeExecutor implements INodeExecutor {

    private class Call {
      private String node;
      private Map<String, List<String>> nodeShardMap;
      private int tryCount;

      @Override
      public String toString() {
        return node + ":" + tryCount + ":" + nodeShardMap;
      }
    }

    private List<Call> calls = new ArrayList<Call>();

    public void execute(String node, Map<String, List<String>> nodeShardMap, int tryCount) {
      Call call = new Call();
      call.node = node;
      call.nodeShardMap = nodeShardMap;
      call.tryCount = tryCount;
      calls.add(call);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      String sep = "";
      for (Call call : calls) {
        sb.append(sep);
        sb.append(call.toString());
        sep = ", ";
      }
      return sb.toString();
    }
  }

  public interface ITestServer extends VersionedProtocol {
    public String testMethod(String param, String[] shards);

    public String testMethodNoShards(String param);

    public String fails(String param, String[] shards);
  }

  private static class TestServer implements ITestServer, InvocationHandler {

    private String node;
    private String param;
    private String[] shards;

    public TestServer(String node) {
      this.node = node;
    }

    public String testMethod(String param, String[] shards) {
      this.param = param;
      this.shards = shards;
      return "bar";
    }

    public String testMethodNoShards(String param) {
      this.param = param;
      this.shards = null;
      return "bar";
    }

    public String fails(String param, String[] shards) {
      throw new RuntimeException("test failure");
    }

    @Override
    public String toString() {
      return node + ":" + param + ":" + (shards != null ? Arrays.asList(shards).toString() : "null");
    }

    public long getProtocolVersion(String arg0, long arg1) {
      return 0;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (name.equals("testMethod")) {
        return testMethod((String) args[0], (String[]) args[1]);
      } else if (name.equals("testMethodNoShards")) {
        return testMethodNoShards((String) args[0]);
      } else if (name.equals("fail")) {
        return fails((String) args[0], (String[]) args[1]);
      } else if (name.equals("toString")) {
        return toString();
      } else {
        throw new RuntimeException("No method " + name + " in TestServer");
      }
    }
  }

  public static class TestProxyProvider implements WorkQueueTest.ProxyProvider {

    private Map<String, VersionedProtocol> proxyCache = new HashMap<String, VersionedProtocol>();
    private Map<String, TestServer> serverCache = new HashMap<String, TestServer>();
    private Set<String> returnNullNodes = new HashSet<String>();

    public VersionedProtocol getProxy(String node) {
      if (returnNullNodes.contains(node)) {
        return null;
      }
      VersionedProtocol vp = proxyCache.get(node);
      if (vp != null) {
        return vp;
      }
      TestServer ts = new TestServer(node);
      serverCache.put(node, ts);
      vp = (VersionedProtocol) Proxy.newProxyInstance(this.getClass().getClassLoader(),
              new Class[] { ITestServer.class }, ts);
      proxyCache.put(node, vp);
      return vp;
    }

    public TestServer getServer(String node) {
      return serverCache.get(node);
    }

    private void returnNullFor(String node) {
      proxyCache.remove(node);
      returnNullNodes.add(node);
    }

    @Override
    public String toString() {
      List<String> nodes = new ArrayList<String>(serverCache.keySet());
      Collections.sort(nodes);
      StringBuilder sb = new StringBuilder();
      String sep = "";
      for (String node : nodes) {
        sb.append(sep);
        sb.append(serverCache.get(node));
        sep = ", ";
      }
      return sb.toString();
    }

  }

}
