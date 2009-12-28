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

import org.junit.Ignore;
import org.junit.Test;

public class LoadTestNodeTest {

  // private static final String INDEX1 = "index1";

  @Test
  @Ignore
  public void testShutdown() throws Exception {
    // TODO: port load test to new client/server setup.
    //
    // LoadTestNode node = startLoadTestNode();
    // node.shutdown();
  }

  @Test
  @Ignore
  public void testStartSearch() throws Exception {
    // TODO: port load test to new client/server setup.
    //
    // startMaster();
    // startNode();
    //
    // DeployClient deployClient = new DeployClient(_zkConf);
    // deployClient.addIndex(INDEX1, TestResources.INDEX1.getAbsolutePath(),
    // 1).joinDeployment();
    //
    // LoadTestNode node = startLoadTestNode();
    // node.initTest(10, new String[] { INDEX1 }, new String[] {"test"}, 10);
    // node.startTest();
    // Thread.sleep(5000);
    // node.stopTest();
    //
    // LoadTestQueryResult[] results = node.getResults();
    // for (LoadTestQueryResult result : results) {
    // assertTrue(result.getEndTime() != -1);
    // }
    //
    // // we should have executed 50 queries in 5s
    // assertTrue(results.length >= 40);
    // assertTrue("Queries per 500ms: " + results.length, results.length <= 60);
    //
    // node.shutdown();
  }
}
