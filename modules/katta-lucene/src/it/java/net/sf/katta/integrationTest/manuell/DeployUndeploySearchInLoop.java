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
package net.sf.katta.integrationTest.manuell;

import net.sf.katta.client.DeployClient;
import net.sf.katta.lib.lucene.Hits;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Ignore;

/**
 * - start a katta-cluster<br>
 * - deploy one or more indices<br>
 * - run this class<br>
 */
@Ignore
public class DeployUndeploySearchInLoop {

  private static Logger LOG = Logger.getLogger(DeployUndeploySearchInLoop.class);

  public static void main(String[] args) throws Exception {
    LuceneClient luceneClient = new LuceneClient();
    ZkConfiguration zkConfig = new ZkConfiguration();
    DeployClient deployClient = new DeployClient(ZkKattaUtil.startZkClient(zkConfig, 60000), zkConfig);

    QueryParser parser = new QueryParser(Version.LUCENE_35, "field", new KeywordAnalyzer());
    Query query = parser.parse("foo: b*");

    int runThroughs = 2;
    while (true) {
      try {
        String indexName = "index" + runThroughs;
        LOG.info("deploying index '" + indexName + "'");
        deployClient.addIndex(indexName, "/Users/jz/Documents/workspace/ms/katta/src/test/testIndexA", 1)
                .joinDeployment();
      } catch (Exception e) {
        logException("deploy", e);
      }

      try {
        String indexName = "index" + (runThroughs - 1);
        LOG.info("undeploying index '" + indexName + "'");
        deployClient.removeIndex(indexName);
      } catch (Exception e) {
        logException("undeploy", e);
      }

      try {
        Hits search = luceneClient.search(query, new String[] { "*" });
        LOG.info(runThroughs + ": got " + search.size() + " results");
      } catch (Exception e) {
        logException("search", e);
      }
      Thread.sleep(5000);
      runThroughs++;
      LOG.info(luceneClient.getClient().getSelectionPolicy());
    }
  }

  private static void logException(String category, Exception e) {
    LOG.error("got " + category + " exception", e);
    // System.out.println("------------THREAD DUMP:");
    // System.out.println(StringUtil.getThreadDump());
    // System.out.println("-------------------------------------");
  }
}
