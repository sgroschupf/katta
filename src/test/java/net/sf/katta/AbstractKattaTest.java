package net.sf.katta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkServer;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ipc.RPC;

import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooKeeper.States;

public abstract class AbstractKattaTest extends TestCase {

  protected final ZkConfiguration conf = new ZkConfiguration();

  List<ZkServer> _startedZkServer = new ArrayList<ZkServer>();

  @Override
  protected final void setUp() throws Exception {
    System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + "#" + getName() + "() ~~~~~~~~~~~~~~~");
    RPC.stopClient();
    cleanZookeeperData(conf);
    onSetUp();
  }

  @Override
  protected final void tearDown() throws Exception {
    for (ZkServer zkServer : _startedZkServer) {
      zkServer.shutdown();
    }
    _startedZkServer.clear();
    RPC.stopClient();
    cleanZookeeperData(conf);
    onTearDown();
  }

  protected void onTearDown() throws Exception {
    // subclasses may override
  }

  protected void onSetUp() throws Exception {
    // subclasses may override
  }

  protected static void cleanZookeeperData(final ZkConfiguration configuration) throws IOException {
    FileUtil.fullyDelete(configuration.getZKDataDir());
    FileUtil.fullyDelete(configuration.getZKDataLogDir());
    FileUtil.fullyDelete(new NodeConfiguration().getShardFolder());
  }

  protected ZkServer createZkServer() throws KattaException {
    ZkServer zkServer = new ZkServer(conf);
    _startedZkServer.add(zkServer);
    return zkServer;
  }

  protected Thread createStartMasterThread(final Master master) {
    Thread thread = new Thread(new Runnable() {
      public void run() {
        try {
          master.start();
        } catch (KattaException e) {
          e.printStackTrace();
        }
      }
    });
    return thread;
  }

  public static Node startNodeServer(final ZKClient client) throws KattaException {
    return startNodeServer(client, null);
  }

  public static Node startNodeServer(final ZKClient client, final String shardFolder) throws KattaException {
    NodeConfiguration configuration = new NodeConfiguration();
    if (null != shardFolder) {
      configuration.setShardFolder(shardFolder);
    }
    final Node node = new Node(client, configuration);
    node.start();
    return node;
  }

  protected void waitForStatus(ZKClient client, ZooKeeper.States state) throws Exception {
    waitForStatus(client, state, conf.getZKTimeOut());
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

  protected void shutdownNode(Node node) {
    node.shutdown();
    try {
      int tryCount = 0;
      int maxTries = 100;
      while (!NetworkUtil.isPortFree(node.getSearchServerPort())) {
        Thread.sleep(100);
        tryCount++;
        if (tryCount >= maxTries) {
          fail("node shutdown but port " + node.getSearchServerPort() + " is still blocked");
        }
      }
    } catch (InterruptedException e) {
      // proceed
    }
  }
}
