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
package net.sf.katta;

import java.io.IOException;

import net.sf.katta.client.DeployClient;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;

public class KattaTest extends AbstractKattaTest {

  private Katta _katta;

  @Override
  protected void onSetUp2() throws Exception {
    super.onSetUp2();
    _katta = new Katta();
  }

  @Override
  protected void onTearDown() throws Exception {
    _katta.close();
    super.onTearDown();
  }

  public void testShowStructure() {
    _katta.showStructure();
  }

  public void testListIndexesWithUnreachableIndex_KATTA_76() throws IOException {
    ZkConfiguration configuration = new ZkConfiguration();
    ZkClient zkClient = new ZkClient(configuration.getZKServers());
    DeployClient deployClient = new DeployClient(zkClient, configuration);
    deployClient.addIndex("index1", "hdfs://localhost:8020/index", 1);
    _katta.listIndex(true, zkClient, configuration);
  }
}
