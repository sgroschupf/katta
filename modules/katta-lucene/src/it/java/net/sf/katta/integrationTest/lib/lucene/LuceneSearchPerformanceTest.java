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
package net.sf.katta.integrationTest.lib.lucene;

import net.sf.katta.client.DeployClient;
import net.sf.katta.integrationTest.support.AbstractLuceneIntegrationTest;
import net.sf.katta.lib.lucene.ILuceneClient;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.lib.lucene.query.TermQueryWritable;
import net.sf.katta.testutil.LuceneTestResources;
import net.sf.katta.testutil.TestResources;
import org.junit.Test;

public class LuceneSearchPerformanceTest extends AbstractLuceneIntegrationTest {

  public LuceneSearchPerformanceTest() {
    super(2);
  }

  @Test
  public void measureSearchPerformance() throws Exception {
    DeployClient deployClient = new DeployClient(_miniCluster.getProtocol());
    deployClient.addIndex("index1", LuceneTestResources.INDEX1.getAbsolutePath(), 1).joinDeployment();
    deployClient.addIndex("index2", TestResources.INDEX2.getAbsolutePath(), 1).joinDeployment();

    final ILuceneClient client = new LuceneClient(_miniCluster.getZkConfiguration());
    final TermQueryWritable query = new TermQueryWritable("foo", "bar");
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      client.search(query, new String[] { "index2", "index1" });
    }
    System.out.println("search took: " + (System.currentTimeMillis() - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < 10000; i++) {
      client.count(query, new String[] { "index2", "index1" });
    }
    System.out.println("count took: " + (System.currentTimeMillis() - start));
    client.close();
  }

}
