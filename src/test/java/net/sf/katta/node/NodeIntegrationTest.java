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

import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class NodeIntegrationTest extends TestCase {

  private ZkServer _zkServer;
  private ZkConfiguration _conf;
  private ZkClient _zkClient;

  @Override
  protected void setUp() {
    _conf = new ZkConfiguration();
    _zkServer = ZkKattaUtil.startZkServer(_conf);
    _zkClient = _zkServer.getZkClient();
  }
  
  @Override
  protected void tearDown() throws Exception {
    _zkServer.shutdown();
  }
  
  public void testShutdown_shouldCleanupZkClientSubscriptions() {
    Node node = new Node(_conf, _zkClient, new LuceneServer());
    node.start();

    _zkClient.subscribeChildChanges("/", new IZkChildListener(){

      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        // ignore
      }});
    
    node.shutdown();
    assertEquals(1, _zkClient.numberOfListeners());
  }
}
