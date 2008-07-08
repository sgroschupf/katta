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
import net.sf.katta.ZkServer;
import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class MasterTest extends TestCase {
  /*
   * create /katta/nodes folder, subscribe notification handle addIndex by
   * distibute shards to nodes handle node failure
   */

  public void testNodes() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    if (client.exists(IPaths.ROOT_PATH)) {
      client.deleteRecursive(IPaths.ROOT_PATH);
    }
    final Master master = new Master(client);
    master.start();
    synchronized (master._client.getSyncMutex()) {
      client.createEphemeral("/katta/nodes/node1");
      client.create(IPaths.NODE_TO_SHARD + "/node1");
      client.createEphemeral("/katta/nodes/node2");
      client.create(IPaths.NODE_TO_SHARD + "/node2");
      client.getSyncMutex().wait();
    }
    assertEquals(2, master.readNodes().size());
    synchronized (master._client.getSyncMutex()) {
      assertTrue(client.delete("/katta/nodes/node1"));
      client.getSyncMutex().wait();
    }
    assertEquals(1, master.readNodes().size());
    client.close();
    zkServer.shutdown();
    Thread.sleep(2000);
  }

  public void testDeployAndRemoveIndex() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    if (client.exists(IPaths.ROOT_PATH)) {
      client.deleteRecursive(IPaths.ROOT_PATH);
    }
    final Master master = new Master(client);
    master.start();
    final File file = new File("./src/test/testIndexA");
    final String path = "file://" + file.getAbsolutePath();
    final IndexMetaData indexMetaData = new IndexMetaData(path, StandardAnalyzer.class.getName(), 2,
        IndexMetaData.IndexState.ANNOUNCED);
    client.createEphemeral("/katta/nodes/node1");
    client.create(IPaths.NODE_TO_SHARD + "/node1");
    client.createEphemeral("/katta/nodes/node2");
    client.create(IPaths.NODE_TO_SHARD + "/node2");

    final String indexPath = "/katta/indexes/indexA";
    client.create(indexPath, indexMetaData);

    // Do not use a wait here. If there is an exception in the event handler a
    // wait gets notified and breaks suddenly.
    Thread.sleep(3000);

    // there should be two servers here now.
    assertEquals(2, client.getChildren(IPaths.NODE_TO_SHARD).size());

    // there should be two shards here now
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/node1").size());

    // there should be two shards here now
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/node2").size());
    // there should be 4 shards indexes now..
    assertEquals(4, client.getChildren(IPaths.SHARD_TO_NODE).size());

    // now we fake the nodes and add the nodes to the shards

    assertEquals(4, client.getChildren(IPaths.SHARD_TO_NODE).size());

    final List<AssignedShard> shards = master.getShardsForIndex("indexA", indexMetaData);
    final List<String> readNodes = master.readNodes();
    final DefaultDistributionPolicy defaultDistributionPolicy = new DefaultDistributionPolicy();
    final Map<String, List<AssignedShard>> ditribute = defaultDistributionPolicy.distribute(client, readNodes, shards,
        2);
    final Set<String> keySet = ditribute.keySet();
    for (final String nodeName : keySet) {
      final List<AssignedShard> toDistribteShards = ditribute.get(nodeName);
      for (final AssignedShard assignedShard : toDistribteShards) {
        client.create(IPaths.SHARD_TO_NODE + "/" + assignedShard.getShardName() + "/" + nodeName);
      }
    }
    Thread.sleep(2000);
    client.showFolders();
    final IndexMetaData metaData = new IndexMetaData();
    client.readData(indexPath, metaData);
    assertEquals(IndexMetaData.IndexState.DEPLOYED, metaData.getState());

    // now remove one node...
    synchronized (client.getSyncMutex()) {
      assertTrue(client.delete("/katta/nodes/node2"));
      client.getSyncMutex().wait(3000);
    }
    assertEquals(4, client.getChildren(IPaths.NODE_TO_SHARD + "/node1").size());
    client.deleteRecursive(indexPath);
    int count = 0;
    while (client.getChildren(IPaths.NODE_TO_SHARD + "/node1").size() != 0) {
      Thread.sleep(500);
      if (count++ > 20) {
        fail("shards are still not removed from node after 20 sec.");
      }
    }
    assertEquals(0, client.getChildren(IPaths.NODE_TO_SHARD + "/node1").size());

    client.close();
    zkServer.shutdown();
  }
}
