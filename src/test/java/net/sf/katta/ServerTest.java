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
package net.sf.katta;

import junit.framework.TestCase;
import net.sf.katta.master.IPaths;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

import com.yahoo.zookeeper.Watcher;
import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooDefs.Ids;
import com.yahoo.zookeeper.proto.WatcherEvent;

public class ServerTest extends TestCase implements Watcher {

  public void testServer() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final String path = "/";
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(conf.getZKServers(), conf.getZKClientPort(), this);
      final String create = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, 0);
      fail("no server yet started");
    } catch (final Exception e) {
      zk.close();
    }

    final ZkServer zkServer = new ZkServer(conf);
    zk = new ZooKeeper(conf.getZKServers(), conf.getZKClientPort(), this);

    final String katta = IPaths.ROOT_PATH;
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    if (client.exists(IPaths.ROOT_PATH)) {
      client.deleteRecursive(IPaths.ROOT_PATH);
    }
    client.close();
    if (zk.exists(katta, false) != null) {
      zk.delete(katta, -1);
    }
    zk.exists(katta, true);
    zk.create(katta, new byte[0], Ids.OPEN_ACL_UNSAFE, 0);

    zk.getChildren(katta, true);
    zk.create("/katta/2", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
    zk.getChildren(katta, true);
    zk.create("/katta/3", new byte[0], Ids.OPEN_ACL_UNSAFE, 0);
    zk.close();

    zkServer.shutdown();
  }

  public void process(final WatcherEvent event) {
    // System.out.println("path: " + event.getPath());
    // System.out.println("type: " + event.getType());
    // System.out.println("state: " + event.getState());
  }

}
