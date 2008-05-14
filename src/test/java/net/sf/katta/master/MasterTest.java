/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.master;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import net.sf.katta.Server;
import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class MasterTest extends TestCase {
  /*
   * create /katta/slaves folder, subscribe notification handle addIndex by
   * distibute shards to slaves handle slave failure
   */

  public void testSlaves() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    if (client.exists(IPaths.ROOT_PATH)) {
      client.deleteRecursiv(IPaths.ROOT_PATH);
    }
    final Master master = new Master(client, new DefaultDistributionPolicy());
    master.start();
    synchronized (master._zk.getSyncMutex()) {
      client.createEphemeral("/katta/slaves/slave1");
      client.create(IPaths.SLAVE_TO_SHARD + "/slave1");
      client.createEphemeral("/katta/slaves/slave2");
      client.create(IPaths.SLAVE_TO_SHARD + "/slave2");
      client.getSyncMutex().wait();
    }
    assertEquals(2, master.readSlaves().size());
    synchronized (master._zk.getSyncMutex()) {
      assertTrue(client.delete("/katta/slaves/slave1"));
      client.getSyncMutex().wait();
    }
    assertEquals(1, master.readSlaves().size());
    client.close();
    server.shutdown();
    Thread.sleep(2000);
  }

  public void testDeployAndRemoveIndex() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    if (client.exists(IPaths.ROOT_PATH)) {
      client.deleteRecursiv(IPaths.ROOT_PATH);
    }
    final Master master = new Master(client, new DefaultDistributionPolicy());
    master.start();
    final File file = new File("./src/test/testIndexA");
    final String path = "file://" + file.getAbsolutePath();
    final IndexMetaData indexMetaData = new IndexMetaData(path, StandardAnalyzer.class.getName(), false);
    client.createEphemeral("/katta/slaves/slave1");
    client.create(IPaths.SLAVE_TO_SHARD + "/slave1");
    client.createEphemeral("/katta/slaves/slave2");
    client.create(IPaths.SLAVE_TO_SHARD + "/slave2");

    final String indexPath = "/katta/indexes/indexA";
    client.create(indexPath, indexMetaData);

    // Do not use a wait here. If there is an exception in the event handler a
    // wait gets notified and breaks suddenly.
    Thread.sleep(3000);

    // there should be two servers here now.
    assertEquals(2, client.getChildren(IPaths.SLAVE_TO_SHARD).size());

    // there should be two shards here now
    assertEquals(2, client.getChildren(IPaths.SLAVE_TO_SHARD + "/slave1").size());

    // there should be two shards here now
    assertEquals(2, client.getChildren(IPaths.SLAVE_TO_SHARD + "/slave2").size());
    // there should be 4 shards indexes now..
    assertEquals(4, client.getChildren(IPaths.SHARD_TO_SLAVE).size());

    // now we fake the slaves and add the slaves to the shards

    assertEquals(4, client.getChildren(IPaths.SHARD_TO_SLAVE).size());

    final List<AssignedShard> shards = master.getShardsForIndex("indexA", indexMetaData);
    final List<String> readSlaves = master.readSlaves();
    final DefaultDistributionPolicy defaultDistributionPolicy = new DefaultDistributionPolicy();
    final Map<String, List<AssignedShard>> ditribute = defaultDistributionPolicy.ditribute(client, readSlaves, shards);
    final Set<String> keySet = ditribute.keySet();
    for (final String slaveName : keySet) {
      final List<AssignedShard> toDistribteShards = ditribute.get(slaveName);
      for (final AssignedShard assignedShard : toDistribteShards) {
        client.create(IPaths.SHARD_TO_SLAVE + "/" + assignedShard.getShardName() + "/" + slaveName);
      }
    }
    Thread.sleep(2000);

    final IndexMetaData metaData = new IndexMetaData();
    client.readData(indexPath, metaData);
    assertTrue(metaData.isDeployed());

    // now remove one slave...
    synchronized (client.getSyncMutex()) {
      assertTrue(client.delete("/katta/slaves/slave2"));
      client.getSyncMutex().wait(3000);
    }
    assertEquals(4, client.getChildren(IPaths.SLAVE_TO_SHARD + "/slave1").size());
    client.deleteRecursiv(indexPath);
    int count = 0;
    while (client.getChildren(IPaths.SLAVE_TO_SHARD + "/slave1").size() != 0) {
      Thread.sleep(500);
      if (count++ > 20) {
        fail("shards are still not removed from slave after 20 sec.");
      }
    }
    assertEquals(0, client.getChildren(IPaths.SLAVE_TO_SHARD + "/slave1").size());

    client.close();
    server.shutdown();
  }
}
