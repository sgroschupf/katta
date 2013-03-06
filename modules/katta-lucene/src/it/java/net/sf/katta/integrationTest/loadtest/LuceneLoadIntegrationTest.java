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
package net.sf.katta.integrationTest.loadtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;

import net.sf.katta.AbstractTest;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.integrationTest.support.KattaMiniCluster;
import net.sf.katta.lib.lucene.LuceneServer;
import net.sf.katta.lib.lucene.query.ILuceneQueryAndFilterWritable;
import net.sf.katta.lib.lucene.query.TermQueryWritable;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.TestIoUtil;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;
import net.sf.katta.tool.loadtest.LoadTestMasterOperation;
import net.sf.katta.tool.loadtest.query.LuceneSearchExecutor;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.SleepServer;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LuceneLoadIntegrationTest extends AbstractTest {

  private static final int NODE_COUNT_LOADTEST = 3;
  private static final int NODE_COUNT_LUCENE = 5;
  protected static final Logger LOG = Logger.getLogger(LuceneLoadIntegrationTest.class);
  // @Rule
  // public PrintMethodNames _printMethodNames = new PrintMethodNames();

  private static ZkServer _zkServer;
  private static KattaMiniCluster _luceneCluster;
  private static KattaMiniCluster _loadtestCluster;

  @BeforeClass
  public static void onBeforeClass() throws Exception {
    ZkConfiguration conf1 = new ZkConfiguration();
    ZkConfiguration conf2 = new ZkConfiguration();
    conf1.setZKRootPath("LuceneLoadIntegrationTest/luceneCluster");
    conf2.setZKRootPath("LuceneLoadIntegrationTest/loadtestCluster");
    cleanup(conf1);
    cleanup(conf2);
    FileUtil.deleteFolder(new NodeConfiguration().getShardFolder());

    _zkServer = ZkKattaUtil.startZkServer(new ZkConfiguration());
    _luceneCluster = new KattaMiniCluster(LuceneServer.class, conf1, NODE_COUNT_LUCENE, 30000);
    _loadtestCluster = new KattaMiniCluster(SleepServer.class, conf2, NODE_COUNT_LOADTEST, 40000);
    _luceneCluster.setZkServer(_zkServer);
    _loadtestCluster.setZkServer(_zkServer);
    _luceneCluster.start();
    _loadtestCluster.start();

    TestUtil.waitUntilLeaveSafeMode(_luceneCluster.getMaster());
    TestUtil.waitUntilLeaveSafeMode(_loadtestCluster.getMaster());
    TestUtil.waitUntilNumberOfLiveNode(_luceneCluster.getProtocol(), NODE_COUNT_LUCENE);
    TestUtil.waitUntilNumberOfLiveNode(_loadtestCluster.getProtocol(), NODE_COUNT_LOADTEST);

    LOG.info("Deploying indices");
    deployIndex(_luceneCluster.getProtocol(), "index1", TestResources.INDEX1);

    // Verify setup.
    // LOG.info("\n\nLUCENE CLUSTER STRUCTURE:\n");
    // _luceneCluster.getProtocol().showStructure(false);
    // LOG.info("\n\nLOADTEST CLUSTER STRUCTURE:\n");
    // _loadtestCluster.getProtocol().showStructure(false);
  }

  private static void cleanup(ZkConfiguration conf) {
    FileUtil.deleteFolder(new File(conf.getZKDataDir()));
    FileUtil.deleteFolder(new File(conf.getZKDataLogDir()));

  }

  @AfterClass
  public static void onAfterClass() throws Exception {
    _luceneCluster.stop(false);
    _loadtestCluster.stop(false);
    _zkServer.shutdown();
  }

  private static void deployIndex(InteractionProtocol protocol, String indexName, File index)
          throws InterruptedException {
    DeployClient deployClient1 = new DeployClient(protocol);
    IIndexDeployFuture deployment = deployClient1.addIndex(indexName, index.getAbsolutePath(), 1);
    LOG.info("Joining deployment on " + deployment.getClass().getName());
    deployment.joinDeployment();
  }

  @Test
  public void testLuceneLoadTest() throws Exception {
    File resultDir = _temporaryFolder.newFolder("results");
    int startRate = 10;
    int endRate = 50;
    int step = 10;
    int runTime = 2000;
    LuceneSearchExecutor queryExecutor = new LuceneSearchExecutor(new String[] { "*" },
        new ILuceneQueryAndFilterWritable[]{new TermQueryWritable("foo", "bar"),
            new TermQueryWritable("notExists", "notExists") }, _luceneCluster.getZkConfiguration(), 200);
    LoadTestMasterOperation loadTestOperation = new LoadTestMasterOperation(NODE_COUNT_LOADTEST, startRate, endRate,
            step, runTime, queryExecutor, resultDir);
    loadTestOperation.registerCompletion(_loadtestCluster.getProtocol());
    _loadtestCluster.getProtocol().addMasterOperation(loadTestOperation);
    loadTestOperation.joinCompletion(_loadtestCluster.getProtocol());

    assertEquals(2, resultDir.list().length);
    File[] listFiles = resultDir.listFiles();
    File logFile;
    File resultFile;
    if (listFiles[0].getName().contains("-results-")) {
      resultFile = listFiles[0];
      logFile = listFiles[1];
    } else {
      resultFile = listFiles[1];
      logFile = listFiles[0];
    }
    assertThat(TestIoUtil.countLines(logFile), almostEquals(300, 50));
    int iterations = 1 + (endRate - startRate) / step;
    assertEquals(1 + iterations, TestIoUtil.countLines(resultFile));
  }

}
