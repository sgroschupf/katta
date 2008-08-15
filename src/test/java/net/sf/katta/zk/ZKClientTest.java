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

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.Text;

public class ZKClientTest extends AbstractKattaTest {

  static Integer _mutex = new Integer(-1);

  public void testWait() throws Exception {
    final ZKClient client = new ZKClient(conf);
    try {
      client.start(500);
      fail("this should fail, since no zk server is yet started.");
    } catch (final Exception e) {
      // expected
    }
    createZkServer();
    client.start(30000);// now should work
    client.close();
  }

  public void testCreateFolder() throws KattaException {
    createZkServer();
    final ZKClient client = new ZKClient(conf);
    final String path = "/katta";
    client.start(10000);
    if (client.exists(path)) {
      assertTrue(client.deleteRecursive(path));
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
    client.deleteRecursive(path);
    client.close();
  }

  public void testChildNotifications() throws Exception {
    createZkServer();
    final ZKClient client = new ZKClient(conf);
    client.start(10000);
    final MyListener listener = new MyListener();
    final String file = "/childFile";
    if (client.exists(file)) {
      client.deleteRecursive(file);
    }
    client.create(file);
    client.subscribeChildChanges(file, listener);
    for (int i = 0; i < 10; i++) {
      synchronized (_mutex) {
        client.create(file + "/" + i);
        _mutex.wait();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
  }

  public void testDataNotifications() throws Exception {
    createZkServer();
    final ZKClient client = new ZKClient(conf);
    client.start(10000);
    final MyListener listener = new MyListener();
    final String katta = "/dataFile";
    if (client.exists(katta)) {
      client.deleteRecursive(katta);
    }
    client.create(katta, new IndexMetaData("path", "someAnalyzr", 3, IndexMetaData.IndexState.ANNOUNCED));
    client.subscribeDataChanges(katta, listener);
    for (int i = 0; i < 10; i++) {
      synchronized (_mutex) {
        final IndexMetaData indexMetaData = new IndexMetaData("path", "someAnalyzr" + i, 3,
            IndexMetaData.IndexState.ANNOUNCED);
        client.writeData(katta, indexMetaData);
        _mutex.wait();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
  }

  public void testGetPath() throws Exception {
    assertEquals("name", ZKClient.getNodeNameFromPath("/foo/bar/name"));
    assertEquals("name", ZKClient.getNodeNameFromPath("/foo/bar/name/"));

  }

  protected class MyListener implements IZkChildListener, IZkDataListener {

    public int _counter = 0;

    public void handleChildChange(String parentPath) throws KattaException {
      handleEvent();
    }

    public void handleDataChange(String parentPath) throws KattaException {
      handleEvent();
    }

    private void handleEvent() {
      _counter++;
      synchronized (_mutex) {
        _mutex.notify();
      }
    }

  }
}
