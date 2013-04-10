package net.sf.katta.integrationTest.support;

import java.io.File;

import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.node.IContentServer;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.ZkConfiguration;

import org.junit.rules.ExternalResource;

/**
 * A container class for a whole katta cluster including:<br>
 * - zk server<br>
 * - master<br>
 * - nodes<br>
 */
public class ClusterRule extends ExternalResource {

  private static int _lastNodeStartPort = 20000;

  private KattaMiniCluster _cluster;
  private Class<? extends IContentServer> _contentServerClass = LuceneServer.class;
  private ZkConfiguration _zkConfiguration = new ZkConfiguration();
  private MasterConfiguration _masterConfiguration = new MasterConfiguration();
  private NodeConfiguration _nodeConfiguration = new NodeConfiguration();
  private int _nodeCount = 2;

  public ZkConfiguration getZkConfiguration() {
    return _zkConfiguration;
  }

  public MasterConfiguration getMasterConfiguration() {
    return _masterConfiguration;
  }

  public NodeConfiguration getNodeConfiguration() {
    return _nodeConfiguration;
  }

  public ClusterRule setContentServer(Class<? extends IContentServer> contentServerClass) {
    _contentServerClass = contentServerClass;
    return this;
  }

  public ClusterRule setNodeCount(int nodeCount) {
    _nodeCount = nodeCount;
    return this;
  }

  public int getNodeCount() {
    return _nodeCount;
  }

  @Override
  protected void before() throws Throwable {
    startCluster();
  }

  @Override
  protected void after() {
    stopCluster();
  }

  public void restartCluster() throws Exception {
    stopCluster();
    startCluster();
  }

  public KattaMiniCluster getCluster() {
    return _cluster;
  }

  private final void startCluster() throws Exception {
    FileUtil.deleteFolder(new File(_zkConfiguration.getZKDataDir()));
    FileUtil.deleteFolder(new File(_zkConfiguration.getZKDataLogDir()));
    FileUtil.deleteFolder(_nodeConfiguration.getShardFolder());

    // start katta cluster
    _cluster = new KattaMiniCluster(_contentServerClass, _zkConfiguration, _nodeCount, _lastNodeStartPort,
            _masterConfiguration, _nodeConfiguration);
    // we permanently start the node on other
    // ports because hadoop rpc seems to make
    // some trouble if a rpc server is restarted on the same port immediately
    _cluster.start();
    _lastNodeStartPort += _cluster.getStartedNodeCount();
  }

  private void stopCluster() {
    _cluster.stop();
  }

}
