package net.sf.katta.master;

import junit.framework.TestCase;
import net.sf.katta.ZkServer;
import net.sf.katta.slave.Slave;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

public class FailTest extends TestCase {

  public void testSlaveFail() throws Exception {
    final ZkConfiguration zkConf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(zkConf);
    final ZKClient client = new ZKClient(zkConf);
    client.waitForZooKeeper(100000);
    final ZKClient slaveClient = new ZKClient(zkConf);
    slaveClient.waitForZooKeeper(100000);
    final ZKClient masterClient = new ZKClient(zkConf);
    masterClient.waitForZooKeeper(100000);
    final ZKClient secMasterClient = new ZKClient(zkConf);
    secMasterClient.waitForZooKeeper(100000);

    final String kattaPath = "/katta";
    if (client.exists(kattaPath)) {
      client.deleteRecursiv(kattaPath);
    }

    final Master master = new Master(masterClient);
    master.start();
    waitFor(client, IPaths.MASTER);

    final Slave slave = new Slave(slaveClient);
    slave.start();
    waitfor(client, IPaths.SLAVES, 1);

    // start secondary master..
    final Master secMaster = new Master(secMasterClient);
    secMaster.start();
    // kill master
    masterClient.close();
    int count = 0;
    while (!secMaster.isMaster() && count++ < 100) {
      Thread.sleep(1000);
    }

    // just make sure we can read the file
    waitFor(client, IPaths.MASTER);

    assertTrue(secMaster.isMaster());

    slaveClient.close();
    zkServer.shutdown();
  }

  private void waitfor(final ZKClient client, final String path, final int size) throws InterruptedException,
  KattaException {
    int count = 0;
    while (client.getChildren(path).size() != size && count++ < 100) {
      Thread.sleep(1000);
    }

  }

  private void waitFor(final ZKClient client, final String path) throws KattaException, InterruptedException {
    int count = 0;
    while (!client.exists(path) && count++ < 100) {
      Thread.sleep(1000);
    }
  }

}
