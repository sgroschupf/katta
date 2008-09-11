package net.sf.katta.zk;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.testutil.Gateway;
import net.sf.katta.util.ZkConfiguration;

import com.yahoo.zookeeper.ZooKeeper;

public class ZkClientReconnectTest extends AbstractKattaTest {

  int GATEWAY_PORT = 2190;
  int ZK_SERVER_PORT = 2181;

  public void testServerDown() throws Exception {
    ZKClient client = new ZKClient(_conf);

    // connect sever and client
    client.start(30000);
    assertTrue(client.getZookeeperState().equals(ZooKeeper.States.CONNECTED));
    System.out.println("test client is connected");

    // disconnect server
    stopZkServer();
    waitForStatus(client, ZooKeeper.States.CONNECTING);
    assertTrue(client.getZookeeperState().equals(ZooKeeper.States.CONNECTING));
    System.out.println("test client is re-connecting");

    // restart server
    startZkServer();
    waitForStatus(client, ZooKeeper.States.CONNECTED);
    assertTrue(client.getZookeeperState().equals(ZooKeeper.States.CONNECTED));
    System.out.println("test client is connected again");

    client.close();
  }

  public void testNetworkDown() throws Exception {
    stopZkServer();
    ZkConfiguration serverConfiguration = new ZkConfiguration();
    ZkConfiguration clientConfiguration = new ZkConfiguration();
    clientConfiguration.setZKServers("localhost:" + GATEWAY_PORT);

    Gateway gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    gateway.start();

    ZkServer server = new ZkServer(serverConfiguration);
    ZKClient client = new ZKClient(clientConfiguration);
    client.start(2000);

    for (int i = 0; i < 3; i++) {
      System.out.println("test reconnect " + i);
      gateway = stopAndStartGateway(gateway, client);
    }

    gateway.interruptAndJoin();
    server.shutdown();
    client.close();
    startZkServer();
  }

  private Gateway stopAndStartGateway(Gateway gateway, ZKClient client) throws Exception {
    waitForStatus(client, ZooKeeper.States.CONNECTED);
    assertEquals(ZooKeeper.States.CONNECTED, client.getZookeeperState());
    gateway.interruptAndJoin();

    waitForStatus(client, ZooKeeper.States.CONNECTING);
    assertEquals(ZooKeeper.States.CONNECTING, client.getZookeeperState());

    gateway = new Gateway(GATEWAY_PORT, ZK_SERVER_PORT);
    gateway.setDaemon(true);
    gateway.start();

    waitForStatus(client, ZooKeeper.States.CONNECTED);
    assertEquals(ZooKeeper.States.CONNECTED, client.getZookeeperState());

    return gateway;
  }

}
