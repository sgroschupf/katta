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
package net.sf.katta.integrationTest.lib.sleep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.sf.katta.client.Client;
import net.sf.katta.client.BasicNodeSelectionPolicy;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.ClassUtil;
import net.sf.katta.util.ClientConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.SleepClient;
import net.sf.katta.util.SleepServer;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Test for {@link SleepClient}.
 */
public class SleepClientTest extends AbstractIntegrationTest {

  protected static final String INDEX1 = "index1";
  protected static final String[] INDEX_1 = { INDEX1 };

  protected static SleepClient _client;

  public SleepClientTest() {
    // TODO: this test will need to use a different index once the Lucene 3 indices are deleted
    super(TestResources.INDEX1.getName() + 0, TestResources.INDEX1, SleepServer.class, 1, false, false);
  }

  @Override
  protected void afterClusterStart() throws Exception {
    IDeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    deployClient.addIndex(INDEX1, TestResources.MAP_FILE_A.getAbsolutePath(), 1).joinDeployment();
    _client = new SleepClient(new BasicNodeSelectionPolicy(), _miniCluster.getZkConfiguration(),
            new ClientConfiguration());
  }

  @AfterClass
  public static void onAfterClass() throws Exception {
    _client.close();
  }

  @Test
  public void testDelay() throws KattaException {
    long start = System.currentTimeMillis();
    _client.sleepIndices(0, INDEX_1);
    long d1 = System.currentTimeMillis() - start;
    System.out.println("time 1 = " + d1);
    start = System.currentTimeMillis();
    _client.sleepIndices(1000, INDEX_1);
    long d2 = System.currentTimeMillis() - start;
    System.out.println("time 2 = " + d2);
    assertTrue(d2 - d1 > 200);
  }

  @Test
  public void testMultiThreadedAccess() throws Exception {
    Random rand = new Random("sleepy".hashCode());
    List<Thread> threads = new ArrayList<Thread>();
    final List<Exception> exceptions = new ArrayList<Exception>();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      final Random rand2 = new Random(rand.nextInt());
      Thread t = new Thread(new Runnable() {
        public void run() {
          for (int j = 0; j < 50; j++) {
            int n = rand2.nextInt(20);
            try {
              _client.sleepIndices(n, INDEX_1);
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
    System.out.println("Took " + (System.currentTimeMillis() - startTime) + " msec.");
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0);
    }
  }

  @Test
  public void testNonExistantShard() throws Exception {
    try {
      _client.sleepShards(0, 0, new String[] { "doesNotExist" });
      fail("Should have failed.");
    } catch (KattaException e) {
      // changed as the default policy does not fail if a non-existent shard is requested, but broadcastToShards will fail when there are no nodes to connect to
      assertEquals("Expected exception", e.getMessage(), "No shards selected: [doesNotExist]");
    }
  }

  @Test
  public void testNonExistantIndex() throws Exception {
    try {
      _client.sleepIndices(0, 0, new String[] { "doesNotExist" });
      fail("Should have failed.");
    } catch (KattaException e) {
      assertEquals("Index [pattern(s)] '[doesNotExist]' do not match to any deployed index: [index1]", e.getMessage());
    }
  }

  @Test
  public void testCleanupOfClientWatchesOnUndeploy_KATTA_182() throws Exception {
    IDeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    IndexMetaData indexMD = deployClient.getIndexMetaData(INDEX1);
    deployClient.removeIndex(INDEX1);
    TestUtil.waitUntilShardsUndeployed(_protocol, indexMD);

    Client client = _client.getClient();
    assertThat(client.getIndices()).excludes((Object) INDEX_1);
    Map<String, Set<Watcher>> existWatches = getExistsWatches(client);
    assertThat(existWatches).isEmpty();

  }

  @SuppressWarnings("unchecked")
  private Map<String, Set<Watcher>> getExistsWatches(Client client) {
    InteractionProtocol protocol = (InteractionProtocol) ClassUtil.getPrivateFieldValue(client, "_protocol");
    ZkClient zkClient = protocol.getZkClient();
    ZkConnection zkConnection = (ZkConnection) ClassUtil.getPrivateFieldValue(zkClient, "_connection");
    ZooKeeper zookeeper = zkConnection.getZookeeper();
    Object watchManager = ClassUtil.getPrivateFieldValue(zookeeper, "watchManager");
    Map<String, Set<Watcher>> existWatches = (Map<String, Set<Watcher>>) ClassUtil.getPrivateFieldValue(watchManager,
            "existWatches");
    return existWatches;
  }
}
