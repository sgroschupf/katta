package net.sf.katta.integrationTest;

import net.sf.katta.integrationTest.support.AbstractLuceneIntegrationTest;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.lib.lucene.query.ILuceneQueryAndFilterWritable;
import net.sf.katta.lib.lucene.query.TermQueryWritable;
import net.sf.katta.protocol.InteractionProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LuceneNodeIntegrationTest extends AbstractLuceneIntegrationTest {
  public LuceneNodeIntegrationTest() {
    super(2);
  }

  @Test
  public void testContentServer() throws Exception {
    deployTestIndices(1, getNodeCount());
    final InteractionProtocol protocol = _miniCluster.getProtocol();
    assertEquals(1, protocol.getIndices().size());

    LuceneClient luceneClient = new LuceneClient(_miniCluster.getZkConfiguration());
    final ILuceneQueryAndFilterWritable query = new TermQueryWritable("body", "text");
    luceneClient.count(query, new String[] { INDEX_NAME });
    luceneClient.close();
  }
}
