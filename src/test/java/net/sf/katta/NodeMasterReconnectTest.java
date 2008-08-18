package net.sf.katta;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.testutil.Gateway;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import com.yahoo.zookeeper.ZooKeeper;

public class NodeMasterReconnectTest extends AbstractKattaTest {

  int GATEWAY_PORT = 2190;
  int ZK_SERVER_PORT = 2181;

  public void testReconnectNode() throws Exception {
    ZkConfiguration gatewayConf = new ZkConfiguration();
    gatewayConf.setZKServers("localhost:" + GATEWAY_PORT);

    // startup the system
    Gateway gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    gateway.start();

    createZkServer();
    ZKClient zkMasterClient = new ZKClient(conf);
    ZKClient zkNodeClient = new ZKClient(gatewayConf);

    Master master = new Master(zkMasterClient);
    Thread masterThread = createStartMasterThread(master);
    masterThread.start();

    Node node = startNodeServer(zkNodeClient);
    masterThread.join();
    assertTrue(zkMasterClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));
    assertTrue(zkNodeClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));

    while (node.getState() == NodeState.STARTING) {
      Thread.sleep(500);
    }

    // check node-master link
    assertEquals("wrong node count:" + master.getNodes(), 1, master.getNodes().size());
    assertEquals(node.getName(), master.getNodes().get(0));

    // now break the node connection
    gateway.interruptAndJoin();
    waitForStatus(zkNodeClient, ZooKeeper.States.CONNECTING);
    synchronized (zkMasterClient.getSyncMutex()) {
      if (master.getNodes().size() != 0) {
        zkMasterClient.getSyncMutex().wait();
      }
    }
    assertEquals(0, master.getNodes().size());

    // now fix the node connection
    gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    gateway.start();
    waitForStatus(zkNodeClient, ZooKeeper.States.CONNECTED);
    synchronized (zkMasterClient.getSyncMutex()) {
      if (master.getNodes().size() != 1) {
        zkMasterClient.getSyncMutex().wait();
      }
    }
    assertEquals(1, master.getNodes().size());

    node.shutdown();
    zkMasterClient.close();
    gateway.interruptAndJoin();
  }
}
