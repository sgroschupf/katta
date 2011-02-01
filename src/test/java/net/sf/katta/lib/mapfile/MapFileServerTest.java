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
package net.sf.katta.lib.mapfile;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.AbstractTest;
import net.sf.katta.node.IContentServer;
import net.sf.katta.testutil.TestResources;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link MapFileServer }.
 */
public class MapFileServerTest extends AbstractTest {

  @SuppressWarnings("unused")
  private static Logger LOG = Logger.getLogger(MapFileServerTest.class);

  private static final String NODE_NAME = "TestNode";
  private static final String SHARD_A_1 = "shard_A_1";
  private static final String SHARD_A_2 = "shard_A_2";
  private static final String SHARD_A_3 = "shard_A_3";
  private static final String SHARD_A_4 = "shard_A_4";
  private static final String SHARD_B_1 = "shard_B_1";
  private static final String SHARD_B_2 = "shard_B_2";

  @Test
  public void testShardA1() throws Exception {
    MapFileServer server = new MapFileServer();
    server.init(NODE_NAME, newNodeConfiguration());
    server.addShard(SHARD_A_1, new File(TestResources.MAP_FILE_A, "a1"));
    assertNotNull(server.getShardMetaData(SHARD_A_1));
    assertEquals("3", server.getShardMetaData(SHARD_A_1).get(IContentServer.SHARD_SIZE_KEY));
    String[] shards = new String[] { SHARD_A_1 };
    assertEquals("This is a test", getOneResult(server, "a.txt", shards));
    assertMissing(server, "d.html", shards);
    assertMissing(server, "v.xml", shards);
    assertMissing(server, "y.xml", shards);
    assertMissing(server, "not-found", shards);
    server.shutdown();
  }

  @Test
  public void testShardA2() throws Exception {
    MapFileServer server = new MapFileServer();
    server.init(NODE_NAME, newNodeConfiguration());
    server.addShard(SHARD_A_2, new File(TestResources.MAP_FILE_A, "a2"));
    assertEquals("3", server.getShardMetaData(SHARD_A_2).get(IContentServer.SHARD_SIZE_KEY));
    String[] shards = new String[] { SHARD_A_2 };
    assertEquals("<b>test</b>", getOneResult(server, "d.html", shards));
    assertMissing(server, "a.txt", shards);
    assertMissing(server, "v.xml", shards);
    assertMissing(server, "y.xml", shards);
    assertMissing(server, "not-found", shards);
    server.shutdown();
  }

  @Test
  public void testMapFile1() throws Exception {
    MapFileServer server = new MapFileServer();
    server.init(NODE_NAME, newNodeConfiguration());
    server.addShard(SHARD_A_1, new File(TestResources.MAP_FILE_A, "a1"));
    server.addShard(SHARD_A_2, new File(TestResources.MAP_FILE_A, "a2"));
    server.addShard(SHARD_A_3, new File(TestResources.MAP_FILE_A, "a3"));
    server.addShard(SHARD_A_4, new File(TestResources.MAP_FILE_A, "a4"));
    assertEquals("3", server.getShardMetaData(SHARD_A_1).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("3", server.getShardMetaData(SHARD_A_2).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("2", server.getShardMetaData(SHARD_A_3).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("4", server.getShardMetaData(SHARD_A_4).get(IContentServer.SHARD_SIZE_KEY));
    String[] shards = new String[] { SHARD_A_1, SHARD_A_2, SHARD_A_3, SHARD_A_4 };
    assertEquals("This is a test", getOneResult(server, "a.txt", shards));
    assertEquals("<b>test</b>", getOneResult(server, "d.html", shards));
    assertEquals("Test in part 3", getOneResult(server, "h.txt", shards));
    assertEquals("test data", getOneResult(server, "k.out", shards));
    assertMissing(server, "v.xml", shards);
    assertMissing(server, "y.xml", shards);
    assertMissing(server, "not-found", shards);
    server.shutdown();
  }

  @Test
  public void testBothMapFiles() throws Exception {
    MapFileServer server = new MapFileServer();
    server.init(NODE_NAME, newNodeConfiguration());
    server.addShard(SHARD_A_1, new File(TestResources.MAP_FILE_A, "a1"));
    server.addShard(SHARD_A_2, new File(TestResources.MAP_FILE_A, "a2"));
    server.addShard(SHARD_A_3, new File(TestResources.MAP_FILE_A, "a3"));
    server.addShard(SHARD_A_4, new File(TestResources.MAP_FILE_A, "a4"));
    server.addShard(SHARD_B_1, new File(TestResources.MAP_FILE_B, "b1"));
    server.addShard(SHARD_B_2, new File(TestResources.MAP_FILE_B, "b2"));
    assertEquals("3", server.getShardMetaData(SHARD_A_1).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("3", server.getShardMetaData(SHARD_A_2).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("2", server.getShardMetaData(SHARD_A_3).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("4", server.getShardMetaData(SHARD_A_4).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("3", server.getShardMetaData(SHARD_B_1).get(IContentServer.SHARD_SIZE_KEY));
    assertEquals("3", server.getShardMetaData(SHARD_B_2).get(IContentServer.SHARD_SIZE_KEY));
    String[] shards = new String[] { SHARD_A_1, SHARD_A_2, SHARD_A_3, SHARD_A_4, SHARD_B_1, SHARD_B_2 };
    String[] mf1Shards = new String[] { SHARD_A_1, SHARD_A_2, SHARD_A_3, SHARD_A_4 };
    String[] mf2Shards = new String[] { SHARD_B_1, SHARD_B_2 };
    assertEquals("This is a test", getOneResult(server, "a.txt", shards));
    assertMissing(server, "a.txt", mf2Shards);
    assertEquals("<b>test</b>", getOneResult(server, "d.html", shards));
    assertMissing(server, "d.html", mf2Shards);
    assertEquals("Test in part 3", getOneResult(server, "h.txt", shards));
    assertMissing(server, "h.txt", mf2Shards);
    assertEquals("test data", getOneResult(server, "k.out", shards));
    assertMissing(server, "k.out", mf2Shards);
    assertEquals("where is test", getOneResult(server, "w.txt", shards));
    assertMissing(server, "w.txt", mf1Shards);
    assertEquals("xrays ionize", getOneResult(server, "x.txt", shards));
    assertMissing(server, "x.txt", mf1Shards);
    assertMissing(server, "not-found", shards);
    server.shutdown();
  }

  @Test
  public void testMultiThreadedAccess() throws Exception {
    final MapFileServer server = new MapFileServer();
    server.init(NODE_NAME, newNodeConfiguration());
    server.addShard(SHARD_A_1, new File(TestResources.MAP_FILE_A, "a1"));
    server.addShard(SHARD_A_2, new File(TestResources.MAP_FILE_A, "a2"));
    server.addShard(SHARD_A_3, new File(TestResources.MAP_FILE_A, "a3"));
    server.addShard(SHARD_A_4, new File(TestResources.MAP_FILE_A, "a4"));
    server.addShard(SHARD_B_1, new File(TestResources.MAP_FILE_B, "b1"));
    server.addShard(SHARD_B_2, new File(TestResources.MAP_FILE_B, "b2"));
    final String[] shards = new String[] { SHARD_A_1, SHARD_A_2, SHARD_A_3, SHARD_A_4, SHARD_B_1, SHARD_B_2 };
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
    Random rand = new Random("katta".hashCode());
    List<Thread> threads = new ArrayList<Thread>();
    final List<Exception> exceptions = new ArrayList<Exception>();
    long startTime = System.currentTimeMillis();
    final AtomicInteger count = new AtomicInteger(0);
    for (int i = 0; i < 20; i++) {
      final Random rand2 = new Random(rand.nextInt());
      Thread t = new Thread(new Runnable() {
        public void run() {
          for (int j = 0; j < 500; j++) {
            int n = rand2.nextInt(entries.size());
            String key = keys.get(n);
            try {
              assertEquals(entries.get(key), getOneResult(server, key, shards));
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

  protected String getOneResult(IMapFileServer server, String key, String[] shards) throws Exception {
    TextArrayWritable texts = server.get(new Text(key), shards);
    assertNotNull(texts);
    assertNotNull(texts.array);
    Writable[] array = texts.array.get();
    assertEquals(1, array.length);
    assertTrue(array[0] instanceof Text);
    Text text = (Text) array[0];
    return text.toString();
  }

  private void assertMissing(IMapFileServer server, String key, String[] shards) throws Exception {
    TextArrayWritable texts = server.get(new Text(key), shards);
    assertNotNull(texts);
    assertNotNull(texts.array);
    Writable[] array = texts.array.get();
    assertEquals(0, array.length);
  }

}
