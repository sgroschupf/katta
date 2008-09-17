/**
 * Copyright 2008 the original author or authors.
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.ExtendedTestCase;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;
import net.sf.katta.zk.ZkServer;
import net.sf.katta.zk.ZKClient.ZkLock;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ipc.RPC;

import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooKeeper.States;

/**
 * Basic katta test which provides some methods for starting master and nodes.
 * Also it always have a zk-server running.
 * 
 */
public abstract class AbstractKattaTest extends ExtendedTestCase {

  private static ZkServer _zkServer;
  protected final ZkConfiguration _conf = new ZkConfiguration();
  private final boolean _resetZkNamespaceBetweenTests;

  public AbstractKattaTest() {
    this(true);
  }

  public AbstractKattaTest(boolean resetZkNamespaceBetweenTests) {
    _resetZkNamespaceBetweenTests = resetZkNamespaceBetweenTests;
  }

  @Override
  protected final void beforeClass() throws Exception {
    cleanZookeeperData(_conf);
    startZkServer();
    resetZkNamespace();
    onBeforeClass();
  }

  @Override
  protected final void afterClass() throws Exception {
    onAfterClass();
    stopZkServer();
    cleanZookeeperData(_conf);
    RPC.stopClient();
  }

  @Override
  protected final void onSetUp() throws Exception {
    if (_resetZkNamespaceBetweenTests) {
      resetZkNamespace();
    }
    onSetUp2();
  }

  private void resetZkNamespace() throws KattaException {
    ZKClient zkClient = new ZKClient(_conf);
    zkClient.start(10000);
    if (zkClient.exists(ZkPathes.ROOT_PATH)) {
      zkClient.deleteRecursive(ZkPathes.ROOT_PATH);
    }
    zkClient.createDefaultNameSpace();
    zkClient.close();
  }

  protected void onBeforeClass() throws Exception {
    // subclasses may override
  }

  protected void onAfterClass() throws Exception {
    // subclasses may override
  }

  protected void onSetUp2() throws Exception {
    // subclasses may override
  }

  protected static void cleanZookeeperData(final ZkConfiguration configuration) throws IOException {
    FileUtil.fullyDelete(configuration.getZKDataDir());
    FileUtil.fullyDelete(configuration.getZKDataLogDir());
    FileUtil.fullyDelete(new NodeConfiguration().getShardFolder());
  }

  protected void startZkServer() throws KattaException {
    if (_zkServer != null) {
      throw new IllegalStateException("zk server already running");
    }
    if (!NetworkUtil.isPortFree(ZkServer.DEFAULT_PORT)) {
      throw new IllegalStateException("port " + ZkServer.DEFAULT_PORT
          + " blocked. Probably other zk server is running.");
    }
    _zkServer = new ZkServer(_conf);
  }

  protected void stopZkServer() {
    if (_zkServer != null) {
      _zkServer.shutdown();
      int waitingTimes = 0;
      try {
        while (!NetworkUtil.isPortFree(ZkServer.DEFAULT_PORT)) {
          if (waitingTimes > 4) {
            throw new IllegalStateException("zk server did not freed it port");
          }
          Thread.sleep(500);
          waitingTimes++;
        }
      } catch (InterruptedException e) {
        // proceed
      }
      _zkServer = null;
    }
  }

  protected MasterStartThread startMaster() throws KattaException {
    ZKClient zkMasterClient = new ZKClient(_conf);
    Master master = new Master(zkMasterClient);
    MasterStartThread masterStartThread = new MasterStartThread(master, zkMasterClient);
    masterStartThread.start();
    return masterStartThread;
  }

  protected NodeStartThread startNode() {
    return startNode(new NodeConfiguration().getShardFolder().getAbsolutePath());
  }

  protected NodeStartThread startNode(String shardFolder) {
    ZKClient zkNodeClient = new ZKClient(_conf);
    NodeConfiguration nodeConf = new NodeConfiguration();
    nodeConf.setShardFolder(shardFolder);
    Node node = new Node(zkNodeClient, nodeConf);
    NodeStartThread nodeStartThread = new NodeStartThread(node, zkNodeClient);
    nodeStartThread.start();
    return nodeStartThread;
  }

  protected void waitForStatus(ZKClient client, ZooKeeper.States state) throws Exception {
    waitForStatus(client, state, _conf.getZKTimeOut());
  }

  protected void waitForStatus(ZKClient client, States state, long timeout) throws Exception {
    long maxWait = System.currentTimeMillis() + timeout;
    while ((maxWait > System.currentTimeMillis())
        && (client.getZookeeperState() == null || client.getZookeeperState() != state)) {
      Thread.sleep(500);
    }
    assertEquals(state, client.getZookeeperState());

  }

  public static void waitForPath(final ZKClient client, final String path) throws KattaException, InterruptedException {
    int tryCount = 0;
    while (!client.exists(path) && tryCount++ < 100) {
      Thread.sleep(500);
    }
    assertTrue("path '" + path + "' does not exists", client.exists(path));
  }

  public static void waitForChilds(final ZKClient client, final String path, final int childCount)
      throws InterruptedException, KattaException {
    int tryCount = 0;
    while (client.getChildren(path).size() != childCount && tryCount++ < 100) {
      Thread.sleep(500);
    }
    assertEquals(childCount, client.getChildren(path).size());
  }

  protected void waitOnLeaveSafeMode(Master master) {
    long startWait = System.currentTimeMillis();
    while (master.isInSafeMode()) {
      if (System.currentTimeMillis() - startWait > 1000 * 60) {
        break;
      }
    }
    assertEquals(false, master.isInSafeMode());
  }

  protected void waitOnNodes(MasterStartThread masterThread, int nodeCount) throws InterruptedException {
    long startWait = System.currentTimeMillis();
    ZKClient zkClient = masterThread.getZkClient();
    ZkLock eventLock = zkClient.getEventLock();
    eventLock.lock();
    while (masterThread.getMaster().getNodes().size() != nodeCount) {
      if (System.currentTimeMillis() - startWait > 1000 * 60) {
        break;
      }
      eventLock.getDataChangedCondition().await(10, TimeUnit.SECONDS);
    }
    eventLock.unlock();
    assertEquals(nodeCount, masterThread.getMaster().getNodes().size());
  }

  protected class MasterStartThread extends Thread {

    private final Master _master;
    private final ZKClient _zkMasterClient;

    public MasterStartThread(Master master, ZKClient zkMasterClient) {
      _master = master;
      _zkMasterClient = zkMasterClient;
    }

    public Master getMaster() {
      return _master;
    }

    public ZKClient getZkClient() {
      return _zkMasterClient;
    }

    public void run() {
      try {
        _master.start();
        waitOnLeaveSafeMode(_master);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void shutdown() {
      _master.shutdown();
    }
  }

  protected class NodeStartThread extends Thread {

    private final Node _node;
    private final ZKClient _client;

    public NodeStartThread(Node node, ZKClient client) {
      _node = node;
      _client = client;
    }

    public Node getNode() {
      return _node;
    }

    public ZKClient getZkClient() {
      return _client;
    }

    public void run() {
      try {
        _node.start();
      } catch (KattaException e) {
        e.printStackTrace();
      }
    }

    public void shutdown() {
      _node.shutdown();
      try {
        int tryCount = 0;
        int maxTries = 100;
        while (!NetworkUtil.isPortFree(_node.getSearchServerPort())) {
          Thread.sleep(100);
          tryCount++;
          if (tryCount >= maxTries) {
            fail("node shutdown but port " + _node.getSearchServerPort() + " is still blocked");
          }
        }
      } catch (InterruptedException e) {
        // proceed
      }
    }

  }
}
