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
package net.sf.katta.zk;

import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.Server;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.Text;

import com.yahoo.zookeeper.proto.WatcherEvent;

public class ZKClientTest extends TestCase {
  static Integer _mutex = new Integer(-1);

  public void testWait() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZKClient client = new ZKClient(conf);
    try {
      client.waitForZooKeeper(500);
      fail("this should fail, since no server is yet started.");
    } catch (final Exception e) {
      ;
    }
    final Server server = new Server(conf);
    client.waitForZooKeeper(3000);// now should work
  }

  public void testCreateFolder() throws KattaException, InterruptedException {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    final String path = "/katta";
    client.waitForZooKeeper(10000);
    if (client.exists(path)) {
      assertTrue(client.deleteRecursiv(path));
    }
    assertFalse(client.exists(path));
    client.create(path);
    assertTrue(client.exists(path));
    assertTrue(client.delete(path));
    final String value = "some value";
    client.create(path, new Text(value));
    final Text text = new Text();
    client.readData(path, text);
    assertEquals(value, text.toString());
    assertTrue(client.exists(path));
    assertTrue(client.delete(path));

    client.create(path);
    List<String> children = client.getChildren(path);
    assertEquals(0, children.size());

    client.create("/katta/child1");
    client.create("/katta/child2");
    children = client.getChildren(path);
    assertEquals(2, children.size());
    client.deleteRecursiv(path);
    client.close();
    Thread.sleep(200);
    server.shutdown();
  }

  public void testChildNotifications() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(10000);
    final MyListener listener = new MyListener();
    final String katta = "/katta";
    if (client.exists(katta)) {
      client.deleteRecursiv(katta);
    }
    client.create(katta);
    client.subscribeChildChanges(katta, listener);
    for (int i = 0; i < 10; i++) {
      synchronized (_mutex) {
        client.create(katta + "/" + i);
        _mutex.wait();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
    Thread.sleep(200);
    server.shutdown();
  }

  public void testDataNotifications() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(10000);
    final MyListener listener = new MyListener();
    final String katta = "/katta";
    if (client.exists(katta)) {
      client.deleteRecursiv(katta);
    }
    client.create(katta, new IndexMetaData("path", "someAnalyzr", false));
    client.subscribeDataChanges(katta, listener);
    for (int i = 0; i < 10; i++) {
      synchronized (_mutex) {
        final IndexMetaData indexMetaData = new IndexMetaData("path", "someAnalyzr" + i, false);
        client.writeData(katta, indexMetaData);
        _mutex.wait();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
    Thread.sleep(200);
    server.shutdown();
  }

  public void testGetPath() throws Exception {
    final ZKClient client = new ZKClient(new ZkConfiguration());
    assertEquals("name", client.getNodeNameFromPath("/foo/bar/name"));
    assertEquals("name", client.getNodeNameFromPath("/foo/bar/name/"));
  }

  private class MyListener implements IZKEventListener {
    public int _counter = 0;

    public void process(final WatcherEvent event) {
      _counter++;
      synchronized (_mutex) {
        _mutex.notify();
      }

    }
  }
}
