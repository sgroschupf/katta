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
package net.sf.katta;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.ISleepClient;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.SleepClient;
import net.sf.katta.util.SleepServer;
import net.sf.katta.util.ZkConfiguration;

import org.apache.log4j.Logger;

/**
 * This class tests the situation where you using 2 instances of Katta to talk
 * to 2 pools of nodes at the same time.
 */
public class MultiInstanceTest extends AbstractKattaTest {

  private static final Logger LOG = Logger.getLogger(MultiInstanceTest.class);
  
  public static final String INDEX1 = "pool1";
  public static final String INDEX2 = "pool2";

  private static List<Node> _nodes1 = new ArrayList<Node>();
  private static List<Node> _nodes2 = new ArrayList<Node>();
  private static Master _master1;
  private static Master _master2;
  private static IDeployClient _deployClient1;
  private static IDeployClient _deployClient2;
  private static ISleepClient _client1;
  private static ISleepClient _client2;

  private static final int POOL_SIZE_1 = 18;
  private static final int POOL_SIZE_2 = 16;

  private static final int NUM_SHARDS_1 = 300;
  private static final int NUM_SHARDS_2 = 150;

  // Don't reset ZK data between each test.
  public MultiInstanceTest() {
    super(false);
  }

  @Override
  protected void onBeforeClass() throws Exception {
    LOG.info("MultiInstanceTest");
    ZkConfiguration conf1 = new ZkConfiguration();
    conf1.setZKRootPath("MultiInstanceTest/pool1");
    ZkConfiguration conf2 = new ZkConfiguration();
    conf2.setZKRootPath("MultiInstanceTest/pool2");

    new DefaultNameSpaceImpl(conf1).createDefaultNameSpace(_zkServer.getZkClient());
    new DefaultNameSpaceImpl(conf2).createDefaultNameSpace(_zkServer.getZkClient());

    NodeConfiguration nConf = new NodeConfiguration();
    int startPort = nConf.getStartPort();
    String shardDir = nConf.getShardFolder().getAbsolutePath();

    // Start waiting for pool1 nodes to appear.
    MasterStartThread masterStartThread1 = startMaster(conf1);
    _master1 = masterStartThread1.getMaster();

    // Create pool1.
    LOG.info("Creating pool 1");
    List<NodeStartThread> nodeThreads1 = new ArrayList<NodeStartThread>();
    for (int i = 0; i < POOL_SIZE_1; i++) {
      NodeStartThread nodeStartThread = startNode(new SleepServer(), startPort, shardDir, conf1);
      nodeThreads1.add(nodeStartThread);
      _nodes1.add(nodeStartThread.getNode());
    }
    masterStartThread1.join();
    for (NodeStartThread nodeStartThread : nodeThreads1) {
      nodeStartThread.join();
    }
    waitOnNodes(masterStartThread1, POOL_SIZE_1);

    // Start waiting for pool1 nodes to appear.
    MasterStartThread masterStartThread2 = startMaster(conf2);
    _master2 = masterStartThread2.getMaster();

    // Create pool2.
    LOG.info("Creating pool 2");
    List<NodeStartThread> nodeThreads2 = new ArrayList<NodeStartThread>();
    for (int i = 0; i < POOL_SIZE_2; i++) {
      NodeStartThread nodeStartThread = startNode(new SleepServer(), startPort, shardDir, conf2);
      nodeThreads2.add(nodeStartThread);
      _nodes2.add(nodeStartThread.getNode());
    }
    masterStartThread2.join();
    for (NodeStartThread nst : nodeThreads1) {
      nst.join();
    }
    waitOnNodes(masterStartThread2, POOL_SIZE_2);

    // Create lots of empty shards. SleepServer does not use the directory, but
    // Node does.
    LOG.info("Creating indicies");
    setupIndex(TestResources.EMPTY1_INDEX, NUM_SHARDS_1);
    setupIndex(TestResources.EMPTY2_INDEX, NUM_SHARDS_2);

    // Deploy shards to pool1.
    LOG.info("Deploying index 1");
    _deployClient1 = new DeployClient(masterStartThread1.getZkClient(), conf1);
    IIndexDeployFuture deployment = _deployClient1.addIndex(INDEX1, TestResources.EMPTY1_INDEX.getAbsolutePath(), 1);
    LOG.info("Joining deployment on " + deployment.getClass().getName());
    deployment.joinDeployment();

    // Deploy shards to pool2.
    LOG.info("Deploying index 2");
    _deployClient2 = new DeployClient(masterStartThread2.getZkClient(), conf2);
    deployment = _deployClient2.addIndex(INDEX2, TestResources.EMPTY2_INDEX.getAbsolutePath(), 1);
    LOG.info("Joining deployment on " + deployment.getClass().getName());
    deployment.joinDeployment();

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

  private void setupIndex(File index, int size) {
    if (index.exists() && index.isDirectory()) {
      for (File f : index.listFiles()) {
        deleteFiles(f);
      }
    }
    for (int i = 0; i < size; i++) {
      File f = new File(index, "shard" + i);
      if(!f.mkdirs()){
        throw new RuntimeException("unable to create folder: "+ f.getAbsolutePath());
      }
    }
  }

  private void deleteFiles(File f) {
    if (f.getName().startsWith(".")) {
      return;
    }
    for (File child : f.listFiles()) {
      deleteFiles(child);
    }
    f.delete();
  }

  @Override
  protected void onAfterClass() throws Exception {
    _client1.close();
    _client2.close();
    for (Node node : _nodes1) {
      node.shutdown();
    }
    for (Node node : _nodes2) {
      node.shutdown();
    }
    _master1.shutdown();
    _master2.shutdown();
    for (File f : TestResources.EMPTY1_INDEX.listFiles()) {
      deleteFiles(f);
    }
    for (File f : TestResources.EMPTY2_INDEX.listFiles()) {
      deleteFiles(f);
    }
  }

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

  public void testParallel() throws InterruptedException {
    LOG.info("Testing multithreaded access to multiple Katta instances...");
    Long start = System.currentTimeMillis();
    Random rand = new Random("Multi katta2".hashCode());
    List<Thread> threads = new ArrayList<Thread>();
    final List<Throwable> throwables = Collections.synchronizedList(new ArrayList<Throwable>());
    for (int i = 0; i < 15; i++) {
      final Random rand2 = new Random(rand.nextInt());
      Thread t = new Thread(new Runnable() {
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
      threads.add(t);
      t.start();
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
