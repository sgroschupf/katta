/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.client;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexDeployError.ErrorType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

  @Test
  public void testJoinIndexDeployment_DeleteIndex() throws Exception {
    String indexName = "indexA";
    IndexDeployFuture deployFuture = new IndexDeployFuture(_protocol, indexName);
    assertEquals(IndexState.DEPLOYING, deployFuture.joinDeployment(200));

    _protocol.publishIndex(new IndexMetaData(indexName, "path", 1));
    _protocol.unpublishIndex(indexName);
    assertEquals(IndexState.DEPLOYED, deployFuture.joinDeployment(200));
  }

}
