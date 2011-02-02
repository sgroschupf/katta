package net.sf.katta.integrationTest;

import net.sf.katta.client.Client;
import net.sf.katta.client.INodeProxyManager;
import net.sf.katta.integrationTest.support.AbstractIntegrationTest;
import net.sf.katta.lib.lucene.ILuceneServer;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.testutil.mockito.ChainedAnswer;
import net.sf.katta.testutil.mockito.PauseAnswer;

import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;

import static org.fest.assertions.Assertions.assertThat;

public class ClientIntegrationTest extends AbstractIntegrationTest {

  public ClientIntegrationTest() {
    super(2);
  }

  @Test(timeout = 20000)
  public void testAddIndex_WithSlowProxyEstablishment() throws Exception {
    Client client = new Client(ILuceneServer.class, _protocol);
    INodeProxyManager proxyCreator = client.getProxyManager();
    INodeProxyManager proxyCreatorSpy = spy(proxyCreator);
    PauseAnswer<Void> pauseAnswer = new PauseAnswer<Void>(null);
    doAnswer(new ChainedAnswer(pauseAnswer, new CallsRealMethods())).when(proxyCreatorSpy).getProxy(anyString(),
            eq(true));
    client.setProxyCreator(proxyCreatorSpy);
    IndexMetaData indexMD = deployIndex(INDEX_NAME, INDEX_FILE, getNodeCount());
    pauseAnswer.joinExecutionBegin();
    assertThat(client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName())).isEmpty();
    assertThat(client.getIndices()).isEmpty();
    pauseAnswer.resumeExecution(true);
    while (client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName()).isEmpty()) {
      Thread.sleep(200);
    }
    assertThat(client.getSelectionPolicy().getShardNodes(indexMD.getShards().iterator().next().getName())).isNotEmpty();
    assertThat(client.getIndices()).isNotEmpty();
  }

}
