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
package net.sf.katta.integrationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.integrationTest.support.KattaMiniCluster;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.ISleepClient;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.SleepClient;
import net.sf.katta.util.SleepServer;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * This class tests the situation where you using 2 instances of Katta to talk
 * to 2 pools of nodes at the same time.
 */
public class MultiClusterTest {

  protected static final Logger LOG = Logger.getLogger(MultiClusterTest.class);
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  public static final String INDEX1 = "pool1";
  public static final String INDEX2 = "pool2";

  private static List<Node> _nodes1 = new ArrayList<Node>();
  private static List<Node> _nodes2 = new ArrayList<Node>();
  private static Master _master1;
  private static Master _master2;
  protected static ISleepClient _client1;
  protected static ISleepClient _client2;

  private static final int POOL_SIZE_1 = 18;
  private static final int POOL_SIZE_2 = 16;

  private static final int NUM_SHARDS_1 = 300;
  private static final int NUM_SHARDS_2 = 150;

  private static ZkServer _zkServer;
  private static KattaMiniCluster _cluster1;
  private static KattaMiniCluster _cluster2;

  @BeforeClass
  public static void onBeforeClass() throws Exception {
    ZkConfiguration conf1 = new ZkConfiguration();
    ZkConfiguration conf2 = new ZkConfiguration();
    conf1.setZKRootPath("MultiClusterTest/pool1");
    conf2.setZKRootPath("MultiClusterTest/pool2");

    _zkServer = ZkKattaUtil.startZkServer(new ZkConfiguration());
    _cluster1 = new KattaMiniCluster(SleepServer.class, conf1, POOL_SIZE_1, 30000);
    _cluster2 = new KattaMiniCluster(SleepServer.class, conf2, POOL_SIZE_2, 40000);
    _cluster1.setZkServer(_zkServer);
    _cluster2.setZkServer(_zkServer);
    _cluster1.start();
    _cluster2.start();

    TestUtil.waitOnLeaveSafeMode(_cluster1.getMaster());
    TestUtil.waitOnLeaveSafeMode(_cluster2.getMaster());
    TestUtil.waitUntilNumberOfLiveNode(_cluster1.getProtocol(), POOL_SIZE_1);
    TestUtil.waitUntilNumberOfLiveNode(_cluster2.getProtocol(), POOL_SIZE_2);

    // Create lots of empty shards. SleepServer does not use the directory, but
    // Node does.
    LOG.info("Creating indicies");
    setupIndex(TestResources.EMPTY1_INDEX, NUM_SHARDS_1);
    setupIndex(TestResources.EMPTY2_INDEX, NUM_SHARDS_2);

    // Deploy shards to pool1.
    LOG.info("Deploying index 1");
    deployIndex(_cluster1.getProtocol(), INDEX1, TestResources.EMPTY1_INDEX);

    // Deploy shards to pool2.
    LOG.info("Deploying index 2");
    deployIndex(_cluster2.getProtocol(), INDEX2, TestResources.EMPTY2_INDEX);

    // Verify setup.
    // LOG.info("\n\nPOOL 1 STRUCTURE:\n");
    // ZKClient tmpClient = new ZKClient(conf1);
    // tmpClient.start(10000);
    // tmpClient.showFolders(false, System.out);
    // LOG.info("\n\nPOOL 2 STRUCTURE:\n");
    // tmpClient = new ZKClient(conf2);
    // tmpClient.start(10000);
    // tmpClient.showFolders(false, System.out);

    // Back end ready to run. Create clients.
    LOG.info("Creating clients");
    _client1 = new SleepClient(conf1);
    _client2 = new SleepClient(conf2);
  }

  @AfterClass
  public static void onAfterClass() throws Exception {
    _client1.close();
    _client2.close();
    _cluster1.stop(false);
    _cluster2.stop(false);
    _zkServer.shutdown();
    for (File f : TestResources.EMPTY1_INDEX.listFiles()) {
      deleteFiles(f);
    }
    for (File f : TestResources.EMPTY2_INDEX.listFiles()) {
      deleteFiles(f);
    }
  }

  private static void deployIndex(InteractionProtocol protocol, String indexName, File index)
          throws InterruptedException {
    DeployClient deployClient1 = new DeployClient(protocol);
    IIndexDeployFuture deployment = deployClient1.addIndex(indexName, index.getAbsolutePath(), 1);
    LOG.info("Joining deployment on " + deployment.getClass().getName());
    deployment.joinDeployment();
  }

  private static void setupIndex(File index, int size) {
    if (index.exists() && index.isDirectory()) {
      for (File f : index.listFiles()) {
        deleteFiles(f);
      }
    }
    for (int i = 0; i < size; i++) {
      File f = new File(index, "shard" + i);
      if (!f.mkdirs()) {
        throw new RuntimeException("unable to create folder: " + f.getAbsolutePath());
      }
    }
  }

  private static void deleteFiles(File f) {
    if (f.getName().startsWith(".")) {
      return;
    }
    for (File child : f.listFiles()) {
      deleteFiles(child);
    }
    f.delete();
  }

  @Test
  public void testSerial() throws KattaException {
    assertEquals(NUM_SHARDS_1, _client1.sleep(0L));
    assertEquals(NUM_SHARDS_2, _client2.sleep(0L));
    //
    Random rand = new Random("Multi katta".hashCode());
    for (int i = 0; i < 200; i++) {
      if (rand.nextBoolean()) {
        assertEquals(NUM_SHARDS_1, _client1.sleep(rand.nextInt(5), rand.nextInt(5)));
      }
      if (rand.nextBoolean()) {
        assertEquals(NUM_SHARDS_2, _client2.sleep(rand.nextInt(5), rand.nextInt(5)));
      }
    }
  }

  @Test
  public void testParallel() throws InterruptedException {
    LOG.info("Testing multithreaded access to multiple Katta instances...");
    Long start = System.currentTimeMillis();
    Random rand = new Random("Multi katta2".hashCode());
    List<Thread> threads = new ArrayList<Thread>();
    final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<Throwable>());
    for (int i = 0; i < 15; i++) {
      final Random rand2 = new Random(rand.nextInt());
      Thread thread = new Thread(new Runnable() {
        public void run() {
          try {
            for (int j = 0; j < 400; j++) {
              if (rand2.nextBoolean()) {
                assertEquals(NUM_SHARDS_1, _client1.sleep(rand2.nextInt(2), rand2.nextInt(2)));
              }
              if (rand2.nextBoolean()) {
                assertEquals(NUM_SHARDS_2, _client2.sleep(rand2.nextInt(2), rand2.nextInt(2)));
              }
            }
          } catch (Throwable t) {
            LOG.error("Error! ", t);
            throwables.add(t);
          }
        }
      });
      threads.add(thread);
      thread.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    LOG.info("Took " + (System.currentTimeMillis() - start) + " msec");
    for (Throwable t : throwables) {
      System.err.println(t);
      t.printStackTrace();
    }
    assertTrue(throwables.isEmpty());
  }

}
