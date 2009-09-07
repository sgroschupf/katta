package net.sf.katta.util;

import java.util.List;

import net.sf.katta.DefaultNameSpaceImpl;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class ZkKattaUtil {

  public static final int DEFAULT_PORT = 2181;

  public static ZkClient startZkClient(ZkConfiguration conf, int connectionTimeout) {
    return new ZkClient(conf.getZKServers(), conf.getZKTimeOut(), connectionTimeout);
  }

  /**
   * @param zkClient
   * @param conf
   * @return all nodes known to the system, also if currently disconnected
   */
  public static List<String> getKnownNodes(ZkClient zkClient, ZkConfiguration conf) {
    return zkClient.getChildren(conf.getZKNodeToShardPath());
  }

  public static ZkServer startZkServer(ZkConfiguration conf) {
    ZkServer zkServer = new ZkServer(conf.getZKDataDir(), conf.getZKDataLogDir(), new DefaultNameSpaceImpl(conf), DEFAULT_PORT, conf
            .getZKTickTime());
    zkServer.start();
    return zkServer;
  }
}
