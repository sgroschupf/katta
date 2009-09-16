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

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.sf.katta.master.Master;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.Node;
import net.sf.katta.testutil.ExtendedTestCase;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

/**
 * Basic katta test which provides some methods for starting master and nodes.
 * Also it always have a zk-server running.
 * 
 */
public abstract class AbstractKattaTest extends ExtendedTestCase {

  protected static ZkServer _zkServer;
  protected final ZkConfiguration _conf;
  private final boolean _resetZkNamespaceBetweenTests;

  public AbstractKattaTest() {
    this(true);
  }

  public AbstractKattaTest(boolean resetZkNamespaceBetweenTests) {
    String confPath = getZkConfigurationResourceName();
    if (confPath == null) {
      _conf = new ZkConfiguration();
    } else {
      System.out.println("Using config file " + confPath);
      _conf = new ZkConfiguration(confPath);
    }
    _resetZkNamespaceBetweenTests = resetZkNamespaceBetweenTests;
  }

  /**
   * Test cases may optionally override this to use an alternate config file for
   * the ZkConfiguration to use. If null is returned, the zero args constructor
   * is used, which defaults to /katta.zk.properties. If non-null then
   * Class.getResourceAsStream() is used to read a Properties file from the
   * path. Returns null.
   * 
   * @return The resource path to use, or null for default.
   */
  protected String getZkConfigurationResourceName() {
    return null;
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
  }

  @Override
  protected final void onSetUp() throws Exception {
    if (_resetZkNamespaceBetweenTests) {
      resetZkNamespace();
    }
    onSetUp2();
  }

  private void resetZkNamespace() {
    ZkClient zkClient = _zkServer.getZkClient();
    zkClient.deleteRecursive(_conf.getZKRootPath());
    new DefaultNameSpaceImpl(_conf).createDefaultNameSpace(zkClient);
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

  protected static void cleanZookeeperData(final ZkConfiguration configuration) {
    String dataDir = configuration.getZKDataDir();
    String dataLogDir = configuration.getZKDataLogDir();
    File shardFolder = new NodeConfiguration().getShardFolder();
    FileUtil.deleteFolder(new File(dataDir));
    FileUtil.deleteFolder(new File(dataLogDir));
    FileUtil.deleteFolder(shardFolder);
    assertFalse(new File(dataDir).exists());
    assertFalse(new File(dataLogDir).exists());
    assertFalse(shardFolder.exists());
  }

  protected void startZkServer() {
    if (_zkServer != null) {
      throw new IllegalStateException("zk server already running");
    }
    int port = _conf.getZKClientPort();
    if (!NetworkUtil.isPortFree(port)) {
      throw new IllegalStateException("port " + port + " blocked. Probably other zk server is running.");
    }
    _zkServer = ZkKattaUtil.startZkServer(_conf);
  }

  protected void stopZkServer() {
    if (_zkServer != null) {
      _zkServer.shutdown();
      _zkServer = null;
      waitUntilPortFree(ZkServer.DEFAULT_PORT, 5000);
    }
  }

  protected void waitUntilPortFree(int port, long maxWaitTime) {
    long startWait = System.currentTimeMillis();
    try {
      while (!NetworkUtil.isPortFree(port)) {
        if (System.currentTimeMillis() - startWait > maxWaitTime) {
          throw new IllegalStateException("port " + port + " blocked");
        }
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      // proceed
    }
  }

  protected MasterStartThread startMaster() throws KattaException {
    return startMaster(_conf);
  }

  protected MasterStartThread startMaster(ZkConfiguration conf) throws KattaException {
    Master master = new Master(conf, _zkServer.getZkClient(), false);
    MasterStartThread masterStartThread = new MasterStartThread(master, _zkServer.getZkClient());
    masterStartThread.start();
    return masterStartThread;
  }

  protected NodeStartThread startNode(INodeManaged server) {
    return startNode(server, new NodeConfiguration().getShardFolder().getAbsolutePath());
  }

  protected NodeStartThread startNode(INodeManaged server, int port) {
    return startNode(server, port, new NodeConfiguration().getShardFolder().getAbsolutePath());
  }

  protected NodeStartThread startNode(INodeManaged server, String shardFolder) {
    NodeConfiguration nodeConf = new NodeConfiguration();
    return startNode(server, nodeConf.getStartPort(), shardFolder);
  }

  protected NodeStartThread startNode(INodeManaged server, int port, String shardFolder) {
    return startNode(server, port, shardFolder, _conf);
  }

  protected NodeStartThread startNode(INodeManaged server, int port, String shardFolder, ZkConfiguration conf) {
    // reuse ZkClient instance
    ZkClient zkNodeClient = ZkKattaUtil.startZkClient(conf, 30000);
    NodeConfiguration nodeConf = new NodeConfiguration();
    nodeConf.setShardFolder(shardFolder);
    nodeConf.setStartPort(port);
    Node node = new Node(conf, zkNodeClient, nodeConf, server);
    NodeStartThread nodeStartThread = new NodeStartThread(node, zkNodeClient);
    nodeStartThread.start();
    return nodeStartThread;
  }

// TODO: port load test to new client/server model.
//
//  protected LoadTestNode startLoadTestNode() throws KattaException {
//    ZkClient zkNodeClient = new ZkClient(_conf);
//    LoadTestNodeConfiguration nodeConf = new LoadTestNodeConfiguration();
//    LoadTestNode node = new LoadTestNode(zkNodeClient, nodeConf);
//    node.start();
//    return node;
//  }

  public static void waitForPath(final ZkClient client, final String path) throws InterruptedException {
    int tryCount = 0;
    while (!client.exists(path) && tryCount++ < 100) {
      Thread.sleep(500);
    }
    assertTrue("path '" + path + "' does not exists", client.exists(path));
  }

  public static void waitForChilds(final ZkClient client, final String path, final int childCount)
          throws InterruptedException {
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

  protected void waitOnNodes(final MasterStartThread masterThread, int nodeCount) throws Exception {
    TestUtil.waitUntil(nodeCount, new Callable<Integer>() {

      @Override
      public Integer call() throws Exception {
        return masterThread.getMaster().getNodes().size();
      }
    }, TimeUnit.SECONDS, 60);

    assertEquals(nodeCount, masterThread.getMaster().getNodes().size());
  }

  protected class MasterStartThread extends Thread {

    private final Master _master;
    private final ZkClient _zkMasterClient;

    public MasterStartThread(Master master, ZkClient zkMasterClient) {
      _master = master;
      _zkMasterClient = zkMasterClient;
      setName(getClass().getSimpleName());
    }

    public Master getMaster() {
      return _master;
    }

    public ZkClient getZkClient() {
      return _zkMasterClient;
    }

    @Override
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
    private final ZkClient _client;

    public NodeStartThread(Node node, ZkClient client) {
      _node = node;
      _client = client;
      setName(getClass().getSimpleName());
    }

    public Node getNode() {
      return _node;
    }

    public ZkClient getZkClient() {
      return _client;
    }

    @Override
    public void run() {
      _node.start();
    }

    public void shutdown() {
      _node.shutdown();
      waitUntilPortFree(_node.getRPCServerPort(), 5000);
    }

  }
}
