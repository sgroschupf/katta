package net.sf.katta.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import net.sf.katta.AbstractZkTest;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexDeployError.ErrorType;

import org.junit.Test;

public class IndexDeployFutureTest extends AbstractZkTest {

  @Test
  public void testJoinIndexDeployment() throws Exception {
    String indexName = "indexA";
    IndexDeployFuture deployFuture = new IndexDeployFuture(_protocol, indexName);
    assertEquals(IndexState.DEPLOYING, deployFuture.joinDeployment(200));

    _protocol.publishIndex(new IndexMetaData(indexName, "path", 1));
    assertEquals(IndexState.DEPLOYED, deployFuture.joinDeployment(200));
  }

  @Test
  public void testJoinIndexErrorDeployment() throws Exception {
    String indexName = "indexA";
    IndexDeployFuture deployFuture = new IndexDeployFuture(_protocol, indexName);
    assertEquals(IndexState.DEPLOYING, deployFuture.joinDeployment(200));

    IndexMetaData indexMD = new IndexMetaData(indexName, "path", 1);
    indexMD.setDeployError(new IndexDeployError(indexName, ErrorType.NO_NODES_AVAILIBLE));
    _protocol.publishIndex(indexMD);
    assertEquals(IndexState.ERROR, deployFuture.joinDeployment(200));
  }

  @Test(timeout = 10000)
  public void testJoinIndexDeploymentAfterZkReconnect() throws Exception {
    final String indexName = "indexA";
    final IndexDeployFuture deployFuture = new IndexDeployFuture(_protocol, indexName);

    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          deployFuture.joinDeployment();
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
      }
    };
    thread.start();
    deployFuture.disconnect();
    deployFuture.reconnect();
    _protocol.publishIndex(new IndexMetaData(indexName, "path", 1));
    thread.join();
  }

}
