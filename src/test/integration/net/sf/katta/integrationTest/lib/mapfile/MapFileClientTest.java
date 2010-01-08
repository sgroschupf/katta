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
package net.sf.katta.integrationTest.lib.mapfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.lib.mapfile.IMapFileClient;
import net.sf.katta.lib.mapfile.MapFileClient;
import net.sf.katta.lib.mapfile.MapFileServer;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Test for {@link MapFileClient}.
 */
public class MapFileClientTest extends AbstractIntegrationTest {

  @SuppressWarnings("unused")
  private static Logger LOG = Logger.getLogger(MapFileClientTest.class);

  private static final String INDEX1 = "index1";
  private static final String INDEX2 = "index2";

  private static final String[] INDEX_1 = { INDEX1 };
  private static final String[] INDEX_2 = { INDEX2 };
  protected static final String[] INDEX_BOTH = { INDEX1, INDEX2 };

  private static IMapFileClient _client;

  public MapFileClientTest() {
    super(MapFileServer.class, 2, false, false);
  }

  @Override
  protected void afterClusterStart() throws Exception {
    IDeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    deployClient.addIndex(INDEX1, TestResources.MAP_FILE_A.getAbsolutePath(), 1).joinDeployment();
    deployClient.addIndex(INDEX2, TestResources.MAP_FILE_B.getAbsolutePath(), 1).joinDeployment();
    _client = new MapFileClient(_miniCluster.getZkConfiguration());
  }

  @AfterClass
  public static void onAfterClass() throws Exception {
    _client.close();
  }

  @Test
  public void testGetA() throws KattaException {
    assertEquals("This is a test", getOneResult("a.txt", INDEX_1));
    assertEquals("1/2/2009: test2", getOneResult("f.log", INDEX_1));
    assertEquals("1/3/2009: more test", getOneResult("g.log", INDEX_1));
    assertEquals("<i>test</i>", getOneResult("i.xml", INDEX_1));
    assertMissing("u.txt", INDEX_1);
    assertMissing("x.txt", INDEX_1);
    assertMissing("not-found", INDEX_1);
  }

  @Test
  public void testGetB() throws KattaException {
    assertEquals("Test U text", getOneResult("u.txt", INDEX_2));
    assertEquals("xrays ionize", getOneResult("x.txt", INDEX_2));
    assertMissing("a.txt", INDEX_2);
    assertMissing("f.log", INDEX_2);
    assertMissing("g.log", INDEX_2);
    assertMissing("i.xml", INDEX_2);
    assertMissing("not-found", INDEX_2);
  }

  @Test
  public void testGetBoth() throws KattaException {
    assertEquals("This is a test", getOneResult("a.txt", INDEX_BOTH));
    assertEquals("1/2/2009: test2", getOneResult("f.log", INDEX_BOTH));
    assertEquals("1/3/2009: more test", getOneResult("g.log", INDEX_BOTH));
    assertEquals("<i>test</i>", getOneResult("i.xml", INDEX_BOTH));
    assertEquals("Test U text", getOneResult("u.txt", INDEX_BOTH));
    assertEquals("xrays ionize", getOneResult("x.txt", INDEX_BOTH));
    assertMissing("not-found", INDEX_BOTH);
  }

  @Test
  public void testMultiThreadedAccess() throws Exception {
    final Map<String, String> entries = new HashMap<String, String>();
    entries.put("a.txt", "This is a test");
    entries.put("b.xml", "<name>test</name>");
    entries.put("d.html", "<b>test</b>");
    entries.put("h.txt", "Test in part 3");
    entries.put("i.xml", "<i>test</i>");
    entries.put("k.out", "test data");
    entries.put("w.txt", "where is test");
    entries.put("x.txt", "xrays ionize");
    entries.put("z.xml", "<zed>foo</zed>");
    final List<String> keys = new ArrayList<String>(entries.keySet());
    Random rand = new Random("Katta".hashCode());
    List<Thread> threads = new ArrayList<Thread>();
    final List<Exception> exceptions = new ArrayList<Exception>();
    long startTime = System.currentTimeMillis();
    final AtomicInteger count = new AtomicInteger(0);
    for (int i = 0; i < 15; i++) {
      final Random rand2 = new Random(rand.nextInt());
      Thread t = new Thread(new Runnable() {
        public void run() {
          for (int j = 0; j < 300; j++) {
            int n = rand2.nextInt(entries.size());
            String key = keys.get(n);
            try {
              assertEquals(entries.get(key), getOneResult(key, INDEX_BOTH));
              count.incrementAndGet();
            } catch (Exception e) {
              System.err.println(e);
              exceptions.add(e);
              break;
            }
          }
        }
      });
      threads.add(t);
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    long time = System.currentTimeMillis() - startTime;
    System.out.println((1000.0 * count.intValue() / time) + " requests / sec");
    assertTrue(exceptions.isEmpty());
  }

  protected String getOneResult(String key, String[] indices) throws KattaException {
    List<String> data = _client.get(key, indices);
    assertNotNull(data);
    assertEquals(1, data.size());
    return data.get(0);
  }

  protected void assertMissing(String key, String[] indices) throws KattaException {
    List<String> data = _client.get(key, indices);
    assertNotNull(data);
    assertTrue(data.isEmpty());
  }

}
