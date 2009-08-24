package net.sf.katta.util;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;

public class ZkKattaUtil {

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
}
