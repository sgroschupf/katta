package net.sf.katta.client;

import java.util.List;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.Katta;
import net.sf.katta.node.Hits;
import net.sf.katta.node.Query;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class ClientFailoverTest extends AbstractKattaTest {

  public void testNodeNotReachable() throws Exception {
    MasterStartThread masterThread = startMaster();
    NodeStartThread nodeThread1 = startNode();
    NodeStartThread nodeThread2 = startNode("/tmp/kattaShards2");

    masterThread.join();
    nodeThread1.join();
    nodeThread2.join();

    // distribute index over 2 nodes
    String index = "index1";
    Katta katta = new Katta();
    katta.addIndex(index, "src/test/testIndexA", StandardAnalyzer.class.getName(), 2);

    // shutdown 2nd node
    nodeThread2.shutdown();
    waitOnNodes(masterThread, 1);

    // simulate 2nd node alive and serving shard
    String node2Name = nodeThread2.getNode().getName();
    ZKClient zkClient = masterThread.getZkClient();
    List<String> shards = zkClient.getChildren(ZkPathes.getIndexPath(index));
    zkClient.create(ZkPathes.getNodePath(node2Name));
    for (String shard : shards) {
      zkClient.create(ZkPathes.getShard2NodePath(shard, node2Name));
    }

    // start search client
    Client searchClient = new Client();
    final Query query = new Query("content:the");
    Hits hits = searchClient.search(query, new String[] { index }, 10);
    assertNotNull(hits);
    assertEquals(10, hits.getHits().size());

    // flip node1/node2 alive status
    nodeThread2 = startNode("/tmp/kattaShards2");
    nodeThread1.shutdown();
    nodeThread2.join();

    // // search again
    // hits = searchClient.search(query, new String[] { index }, 10);
    // assertNotNull(hits);
    // assertEquals(10, hits.getHits().size());

    searchClient.close();
    nodeThread1.shutdown();
    masterThread.shutdown();
  }

}
