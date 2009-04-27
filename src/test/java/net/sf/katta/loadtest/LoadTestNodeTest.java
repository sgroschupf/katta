package net.sf.katta.loadtest;

import java.io.IOException;
import java.util.List;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.client.DeployClient;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class LoadTestNodeTest extends AbstractKattaTest {

  private static final String INDEX1 = "index1";

  public void testShutdown() throws KattaException {
    LoadTestNode node = startLoadTestNode();
    node.shutdown();
  }

  public void testStartSearch() throws KattaException, InterruptedException, IOException {
    startMaster();
    startNode();
    
    DeployClient deployClient = new DeployClient(_conf);
    deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(), StandardAnalyzer.class.getName(), 1)
        .joinDeployment();

    LoadTestNode node = startLoadTestNode();
    node.startTest(1, new String[] { INDEX1 }, "test", 10);
    Thread.sleep(500);
    node.stopTest();
    
    List<Integer> results = node.getResults();
    for (Integer result : results) {
      assertTrue(result != -1);
    }
    assertTrue(results.size() > 0);
    
    node.shutdown();
  }
}
