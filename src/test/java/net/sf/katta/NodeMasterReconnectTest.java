package net.sf.katta;

import java.util.concurrent.TimeUnit;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
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

    MasterStartThread masterStartThread = startMaster();
    Master master = masterStartThread.getMaster();
    ZKClient zkNodeClient = new ZKClient(gatewayConf);
    Node node = new Node(zkNodeClient);
    node.start();
    masterStartThread.join();
    ZKClient zkMasterClient = masterStartThread.getZkClient();

    assertTrue(zkMasterClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));
    assertTrue(zkNodeClient.getZookeeperState().equals(ZooKeeper.States.CONNECTED));

    // check node-master link
    assertEquals("wrong node count:" + master.getNodes(), 1, master.getNodes().size());
    assertTrue(master.getNodes().contains(node.getName()));

    // now break the node connection
    zkMasterClient.getEventLock().lock();
    gateway.interruptAndJoin();
    zkMasterClient.getEventLock().getDataChangedCondition().await(20, TimeUnit.SECONDS);
    zkMasterClient.getEventLock().unlock();
    assertEquals(0, master.getNodes().size());

    // now fix the node connection
    gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    zkMasterClient.getEventLock().lock();
    gateway.start();
    zkMasterClient.getEventLock().getDataChangedCondition().await(20, TimeUnit.SECONDS);
    zkMasterClient.getEventLock().unlock();
    assertEquals(1, master.getNodes().size());

    node.shutdown();
    masterStartThread.shutdown();
    gateway.interruptAndJoin();
  }
}
