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
package net.sf.katta.node;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.ZkTestSystem;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Rule;
import org.junit.Test;

public class NodeIntegrationTest {

  @Rule
  public ZkTestSystem _zk = ZkTestSystem.getInstance();
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  @Test
  public void testShutdown_shouldCleanupZkClientSubscriptions() {
    ZkClient zkClient = _zk.getZkClient();
    Node node = new Node(_zk.getInteractionProtocol(), new LuceneServer());
    node.start();

    zkClient.subscribeChildChanges("/", new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        // ignore
      }
    });

    node.shutdown();
    assertEquals(1, zkClient.numberOfListeners());
  }

}
