package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import net.sf.katta.AbstractKattaTest.NodeStartThread;
import net.sf.katta.master.Master;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.protocol.operation.leader.MockedMasterNodeTest;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;

public class AbstractMasterNodeTest extends MockedMasterNodeTest {

  protected ZkConfiguration _conf = _zk.getZkConf();
  protected NodeConfiguration _nodeConf;

  protected MasterStartThread startMaster() throws KattaException {
    return startMaster(_zk.getZkConf());
  }

  protected MasterStartThread startMaster(ZkConfiguration conf) throws KattaException {
    return startMaster(conf, new MasterConfiguration());
  }

  protected MasterStartThread startMaster(ZkConfiguration conf, MasterConfiguration masterConf) throws KattaException {
    Master master = new Master(_protocol, false, masterConf);
    MasterStartThread masterStartThread = new MasterStartThread(master, _zk.getZkClient());
    masterStartThread.start();
    return masterStartThread;
  }

  protected NodeStartThread startNode(INodeManaged server) {
    if (_nodeConf == null) {
      _nodeConf = new NodeConfiguration();
    }
    return startNode(server, _nodeConf.getShardFolder().getAbsolutePath());
  }

  protected NodeStartThread startNode(INodeManaged server, String shardFolder) {
    if (_nodeConf == null) {
      _nodeConf = new NodeConfiguration();
    }
    // return startNode(server, _nodeConf.getStartPort(), shardFolder);
    return null;
  }

  public static void waitForChilds(final ZkClient client, final String path, final int childCount)
          throws InterruptedException {
    int tryCount = 0;
    while (client.getChildren(path).size() != childCount && tryCount++ < 100) {
      Thread.sleep(500);
    }
    assertEquals(childCount, client.getChildren(path).size());
  }

  public class MasterStartThread extends Thread {

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
        TestUtil.waitOnLeaveSafeMode(_master);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void shutdown() {
      _master.shutdown();
    }
  }
}
