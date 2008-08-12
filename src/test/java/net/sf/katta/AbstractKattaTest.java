package net.sf.katta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.master.Master;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ipc.RPC;

public abstract class AbstractKattaTest extends TestCase {

  protected final ZkConfiguration conf = new ZkConfiguration();

  List<ZkServer> _startedZkServer = new ArrayList<ZkServer>();

  @Override
  protected final void setUp() throws Exception {
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

  protected ZkServer createZkServer() throws KattaException {
    ZkServer zkServer = new ZkServer(conf);
    _startedZkServer.add(zkServer);
    return zkServer;
  }
}
