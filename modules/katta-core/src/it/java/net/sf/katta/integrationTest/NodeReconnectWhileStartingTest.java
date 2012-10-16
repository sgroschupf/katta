package net.sf.katta.integrationTest;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.node.IContentServer;
import net.sf.katta.node.Node;
import net.sf.katta.operation.master.IndexDeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.util.NodeConfiguration;

public class NodeReconnectWhileStartingTest extends AbstractIntegrationTest {

  public static class TestContentServer implements IContentServer {
    Set<String> shards = new HashSet<String>();
    
    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
      return 0;
    }

    @Override
    public void init(String nodeName, NodeConfiguration nodeConfiguration) {
      
    }

    @Override
    public void addShard(String shardName, File shardDir) throws Exception {
      addCallBlock();
      shards.add(shardName);
    }

    @Override
    public void removeShard(String shardName) throws Exception {
      shards.remove(shardName);
    }

    @Override
    public Collection<String> getShards() {
      return Collections.unmodifiableSet(shards);
    }

    @Override
    public Map<String, String> getShardMetaData(String shardName)
        throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public void shutdown() throws Exception {
      shards.clear();
    }
    
  }
  
  public NodeReconnectWhileStartingTest() {
    super(TestContentServer.class, 1);
  }
  
  public static void addCallBlock() throws InterruptedException {
    addCalls++;
    while (addCalls == 5) Thread.sleep(50);
  }
  
  private static volatile int addCalls = 0;
  
  @Test
  public void testReconnectNodeDuringInit() throws Exception {
    final InteractionProtocol protocol = _miniCluster.getProtocol();

    IndexDeployOperation deployOperation = new IndexDeployOperation(INDEX_NAME, "file://"
            + INDEX_FILE.getAbsolutePath(), getNodeCount());
    protocol.addMasterOperation(deployOperation);
    TestUtil.waitUntilIndexDeployed(protocol, INDEX_NAME);
    
    final Node[] newNode = new Node[1];
    final Exception[] childException = new Exception[1];
    Thread t = new Thread() {
      @Override
      public void run() {
        Node shutdownNode = _miniCluster.getNode(0);
        NodeConfiguration nodeConfiguration = new NodeConfiguration(_miniCluster.getDefaultNodeConfiguration().getPropertiesCopy());
        nodeConfiguration.setStartPort(shutdownNode.getRPCServerPort());
        _miniCluster.shutdownNode(0);
        newNode[0] = new Node(_protocol, nodeConfiguration, shutdownNode.getContext().getContentServer());
        try {
          newNode[0].start();
        } catch (Exception e) {
          childException[0] = e;
        }
      }
    };
    t.start();
    
    /* The 5th call to waitOnAddIfDesired() is the first addShard call from the
     * second thread.
     * Don't continue until that call occurs.
     */
    while (addCalls < 5) Thread.sleep(50);
    
    // Before KATTA-216 fix in Node.disconnect(): NPE thrown on _nodeOperatorThread.interrupt()
    // After KATTA-216 fix: Child thread fails to restart the node due to already exists exception..
    newNode[0].disconnect();
    newNode[0].reconnect();
    
    addCalls++;
    
    t.join();
    /* Child thread should fail something like this:
     * org.I0Itec.zkclient.exception.ZkNodeExistsException: org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists for /katta/shard-to-nodes/testIndexA0#bIndex/localhost:20000
    if (childException[0] != null) {
      throw childException[0];
    }
    */
  }
}
