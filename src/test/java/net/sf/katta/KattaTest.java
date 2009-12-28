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

import net.sf.katta.client.DeployClient;
import net.sf.katta.master.Master;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.junit.Test;

public class KattaTest extends AbstractZkTest {

  @Test
  public void testShowStructure() {
    Katta katta = new Katta(_zk.getZkConf(), _zk.getZkClient());
    katta.showStructure(false);
    katta.showStructure(true);
  }

  @Test
  public void testShowErrorsWithIndexNotExist() {
    Katta katta = new Katta(_zk.getZkConf(), _zk.getZkClient());
    katta.showErrors("abc");
  }

  @Test
  public void testListIndexesWithUnreachableIndex_KATTA_76() {
    Katta _katta = new Katta(_zk.getZkConf(), _zk.getZkClient());
    DeployClient deployClient = new DeployClient(_protocol);
    deployClient.addIndex("index1", "hdfs://localhost:8020/index", 1);
    _katta.listIndex(true);
  }

  @Test
  public void testEmbeddedZK() throws Exception {
    // by default there need to be a zkserver
    final ZkConfiguration conf = new ZkConfiguration();

    Master master = Katta.startMaster(conf);
    try {
      ZkClient client = ZkKattaUtil.startZkClient(conf, 10000);
      client.close();
    } finally {
      if (master != null) {
        master.shutdown();
      }
    }
  }

  @Test
  public void testNoEmbeddedZK() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    conf.setEmbedded(false);

    // we start our own zkServer that the master will connect to
    ZkServer zkServer = ZkKattaUtil.startZkServer(conf);
    try {
      // this would fail if this would try to start another ZkServer on the same
      // port
      Master master = Katta.startMaster(conf);
      master.shutdown();
    } finally {
      zkServer.shutdown();
    }
  }
}
