package net.sf.katta.integrationTest.lib.lucene;

import net.sf.katta.client.NodeProxyManager;
import net.sf.katta.client.ShardAccessException;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.lib.lucene.Hits;
import net.sf.katta.lib.lucene.LuceneClient;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class LuceneClientFailoverTest extends AbstractIntegrationTest {

  public LuceneClientFailoverTest() {
    super(2);
  }

  @Test
  public void testSearchAndCount_NodeProxyDownAfterClientInitialization() throws Exception {
    deployTestIndices(1, getNodeCount());
    // start search client
    LuceneClient luceneClient = new LuceneClient(_miniCluster.getZkConfiguration());

    // shutdown proxy of node1
    _miniCluster.getNode(0).getRpcServer().stop();

    final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
    System.out.println("=========================");
    assertSearchResults(10, luceneClient.search(query, new String[] { INDEX_NAME }, 10));
    assertEquals(937, luceneClient.count(query, new String[] { INDEX_NAME }));
    assertSearchResults(10, luceneClient.search(query, new String[] { INDEX_NAME }, 10));
    assertEquals(937, luceneClient.count(query, new String[] { INDEX_NAME }));
    // search 2 time to ensure we get all availible nodes
    System.out.println("=========================");
    _miniCluster.shutdownNode(0);
    luceneClient.close();
  }

  @Test
  public void testGetDetails_NodeProxyDownAfterClientInitialization() throws Exception {
    deployTestIndices(1, getNodeCount());
    LuceneClient luceneClient = new LuceneClient(_miniCluster.getZkConfiguration());
    ((NodeProxyManager) luceneClient.getClient().getProxyManager()).setSuccessiveProxyFailuresBeforeReestablishing(1);
    final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
    Hits hits = luceneClient.search(query, new String[] { INDEX_NAME }, 10);

    // shutdown proxy of node1
    System.out.println("=========================");
    if (_miniCluster.getNode(0).getName().equals(hits.getHits().get(0).getNode())) {
      _miniCluster.shutdownNodeRpc(0);
    } else {
      _miniCluster.shutdownNodeRpc(1);
    }
    assertFalse(luceneClient.getDetails(hits.getHits().get(0)).isEmpty());
    assertFalse(luceneClient.getDetails(hits.getHits().get(0)).isEmpty());
    // search 2 time to ensure we get all available nodes
    System.out.println("=========================");
    shutdownNodes();
    luceneClient.close();
  }

  @Test
  public void testAllNodeProxyDownAfterClientInitialization() throws Exception {
    deployTestIndices(1, getNodeCount());
    LuceneClient luceneClient = new LuceneClient(_miniCluster.getZkConfiguration());
    final Query query = new QueryParser(Version.LUCENE_30, "", new KeywordAnalyzer()).parse("content: the");
    for (int i = 0; i < _miniCluster.getRunningNodeCount(); i++) {
      _miniCluster.shutdownNodeRpc(i);
    }

    System.out.println("=========================");
    try {
      luceneClient.search(query, new String[] { INDEX_NAME }, 10);
      fail("should throw exception");
    } catch (ShardAccessException e) {
      // expected
    }
    System.out.println("=========================");
    shutdownNodes();
    luceneClient.close();
  }

  private void assertSearchResults(int expectedResults, Hits hits) {
    assertNotNull(hits);
    assertEquals(expectedResults, hits.getHits().size());
  }
}
