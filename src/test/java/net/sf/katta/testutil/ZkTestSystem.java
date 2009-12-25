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
package net.sf.katta.testutil;

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

public class ZkTestSystem extends ExternalResource {

  protected static final Logger LOG = Logger.getLogger(ZkTestSystem.class);

  private static int PORT = 10001;
  private static ZkTestSystem _instance;
  private ZkServer _zkServer;
  private ZkConfiguration _conf;

  private ZkTestSystem() {
    System.out.println("~~~~~~~~~~~~~~~ starting zk system ~~~~~~~~~~~~~~~");
    String baseDir = "build/zkdata";
    String dataDir = baseDir + "/data";
    String logDir = baseDir + "/log";
    _conf = new ZkConfiguration();
    _conf.setZKServers("localhost:" + PORT);
    _conf.setZKRootPath("/zk_testsystem");
    _zkServer = new ZkServer(dataDir, logDir, new DefaultNameSpaceImpl(_conf), PORT);
    _zkServer.start();
    System.out.println("~~~~~~~~~~~~~~~ zk system started ~~~~~~~~~~~~~~~");
  }

  @Override
  // executed before every test method
  protected void before() throws Throwable {
    cleanupZk();
  }

  @Override
  // executed after every test method
  protected void after() {
    cleanupZk();
  }

  private void cleanupZk() {
    LOG.info("cleanup zk namespace");
    getZkClient().deleteRecursive(_conf.getZkRootPath());
    new DefaultNameSpaceImpl(_conf).createDefaultNameSpace(getZkClient());
    LOG.info("unsubscribing " + getZkClient().numberOfListeners() + " listeners");
    getZkClient().unsubscribeAll();
  }

  public static ZkTestSystem getInstance() {
    if (_instance == null) {
      _instance = new ZkTestSystem();
      _instance.cleanupZk();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOG.info("shutting zk down");
          getInstance().getZkServer().shutdown();
        }
      });
    }
    return _instance;
  }

  public ZkServer getZkServer() {
    return _zkServer;
  }

  public ZkClient getZkClient() {
    return _zkServer.getZkClient();
  }

  public InteractionProtocol getInteractionProtocol() {
    return new InteractionProtocol(_zkServer.getZkClient(), _conf);
  }

  public InteractionProtocol createInteractionProtocol() {
    return new InteractionProtocol(createZkClient(), _conf);
  }

  public ZkConfiguration getZkConf() {
    return _conf;
  }

  public int getServerPort() {
    return PORT;
  }

  public ZkClient createZkClient() {
    return new ZkClient("localhost:" + PORT);
  }

  public void showStructure() {
    getInteractionProtocol().showStructure(true);
  }

  // LuceneComplianceTest (zk, lucene server)
  // LuceneClientTest (zk, lucene server)
  // MasterTest(zk, ~lucene server)
  // NodeTest(zk, lucene server)
  // AlternateRootCfgClientTest == LuceneClientTest with other configuration
  // AlternateRootCfgMasterTest == MasterTest
  // AlternateRootCfgNodeTest == NodeTest
  // √ KattaTest (zk)
  // PerformanceTest (zk, lucene server)
  // LuceneClientFailoverTest (~zk, lucene server)
  // MapFileClientTest (zk, mapfile server)
  // SleepClientTest (zk, sleep server)
  // √ EmbeddedZookeeperTest (~zk)
  // FailTest (zk, lucene server)
  // √ NodeIntegrationTest (zk, lucene server)

  // NOT:
  // MultiInstanceTest (zk?, custom)
  // NodeMasterReconnectTest ( custom)
  // ClientResultTest (no dep)
  // DefaultNodeSelectionPolicyTest (no dep)
  // NodeInteractionTest (no dep)
  // ResultCompletePolicyTest (no dep)
  // WorkQueueTest (no dep)
  // FieldSortComparatorTest
  // Ec2ServiceTest
  // SampleIndexGeneratorTest
  // DefaultDistributionPolicyTest
  // LowestShardCountDistributionPolicyTest
  // JmxMonitorTest
  // DocumentFrequencyWritableTest
  // LuceneServerTest
  // MapFileServerTest
  // SleepServerTest
  // CircularListTest
  // FileUtilTest
  // One2ManyListMapTest
  // WritableTypeTest
  // ZkConfigurationTest

  // UNCOMMENTED:
  // LoadTestNodeTest
  // LoadTestStarterTest

  // TIMES:
  // 5 minutes 21 seconds
  // 5 minutes 29/41 seconds (fixed 3 test with ZkTestSystem)
}
