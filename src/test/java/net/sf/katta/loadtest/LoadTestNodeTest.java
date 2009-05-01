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
package net.sf.katta.loadtest;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.client.DeployClient;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.util.KattaException;

public class LoadTestNodeTest extends AbstractKattaTest {

  private static final String INDEX1 = "index1";

  public void testShutdown() throws KattaException {
    LoadTestNode node = startLoadTestNode();
    node.shutdown();
  }

  public void testStartSearch() throws KattaException, InterruptedException {
    startMaster();
    startNode();

    DeployClient deployClient = new DeployClient(_conf);
    deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(), 1).joinDeployment();

    LoadTestNode node = startLoadTestNode();
    node.startTest(10, new String[] { INDEX1 }, "test", 10);
    Thread.sleep(5000);
    node.stopTest();

    LoadTestQueryResult[] results = node.getResults();
    for (LoadTestQueryResult result : results) {
      assertTrue(result.getEndTime() != -1);
    }

    // we should have executed 50 queries in 5s
    assertTrue(results.length >= 45);
    assertTrue("Queries per 500ms: " + results.length, results.length <= 55);

    node.shutdown();
  }
}
