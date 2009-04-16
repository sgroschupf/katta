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
package net.sf.katta.zk;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.AbstractKattaTest;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class ZKClientTest extends AbstractKattaTest {

  public void testStart() throws Exception {
    stopZkServer();
    final ZKClient client = new ZKClient(_conf);
    try {
      client.start(500);
      fail("this should fail, since no zk server is yet started.");
    } catch (final Exception e) {
      // expected
    }
    startZkServer();
    client.start(30000);// now should work
    client.close();
  }

  public void testShowStructure() throws KattaException {
    final ZKClient client = new ZKClient(_conf);
    client.start(10000);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    client.showFolders(outputStream);
    String output = new String(outputStream.toByteArray());
    assertTrue(output.contains("+katta"));
    assertTrue(output.contains("+node-to-shard"));
  }

  public void testCreateFolder() throws KattaException {
    final ZKClient client = new ZKClient(_conf);
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
    final ZKClient client = new ZKClient(_conf);
    client.start(10000);
    final MyListener listener = new MyListener();
    final String file = "/childFile";
    if (client.exists(file)) {
      client.deleteRecursive(file);
    }
    client.create(file);
    client.subscribeChildChanges(file, listener);
    for (int i = 0; i < 10; i++) {
      client.getEventLock().lock();
      try {
        client.create(file + "/" + i);
        client.getEventLock().getDataChangedCondition().await();
      } finally {
        client.getEventLock().unlock();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
  }

  public void testDataNotifications() throws Exception {
    final ZKClient client = new ZKClient(_conf);
    client.start(10000);
    final MyListener listener = new MyListener();
    final String katta = "/dataFile";
    if (client.exists(katta)) {
      client.deleteRecursive(katta);
    }
    client.create(katta, new IndexMetaData("path", "someAnalyzr", 3, IndexMetaData.IndexState.ANNOUNCED));
    client.subscribeDataChanges(katta, listener);
    for (int i = 0; i < 10; i++) {
      client.getEventLock().lock();
      try{
        final IndexMetaData indexMetaData = new IndexMetaData("path", "someAnalyzr" + i, 3,
            IndexMetaData.IndexState.ANNOUNCED);
        client.writeData(katta, indexMetaData);
        client.getEventLock().getDataChangedCondition().await();
      } finally {
        client.getEventLock().unlock();
      }
    }
    assertEquals(10, listener._counter);
    client.close();
  }

  public void testWatchProcessThreadSafeness() throws Exception {
    final ZKClient zkClient = new ZKClient(_conf);
    zkClient.start(5000);
    final String path = "/";
    final int listenerCount = 20;
    final int threadCount = 10;
    final int watchEventsPerThread = 100;
    final Mockery mockery = new Mockery();
    final List<IZkChildListener> childListeners = new ArrayList<IZkChildListener>();
    for (int i = 0; i < listenerCount; i++) {
      childListeners.add(mockery.mock(IZkChildListener.class, "cl" + i));
    }
    mockery.checking(new Expectations() {
      {
        for (final IZkChildListener childListener : childListeners) {
          atLeast(1).of(childListener).handleChildChange(with(equal(path)), with(aNonNull(List.class)));
        }
      }
    });

    final AtomicInteger concurrentModificationExceptions = new AtomicInteger();
    final AtomicInteger unknownExceptions = new AtomicInteger();
    final Runnable fireWatchEventsRunnable = new Runnable() {
      public void run() {
        try {
          for (int i = 0; i < watchEventsPerThread; i++) {
            zkClient.process(new WatchedEvent(EventType.NodeChildrenChanged ,KeeperState.SyncConnected, path));
            for (final IZkChildListener childListener : childListeners) {
              zkClient.unsubscribeChildChanges(path, childListener);
              zkClient.subscribeChildChanges(path, childListener);
            }
          }
        } catch (final ConcurrentModificationException e) {
          concurrentModificationExceptions.incrementAndGet();
          e.printStackTrace();
        } catch (final Exception e) {
          unknownExceptions.incrementAndGet();
          e.printStackTrace();
        }
      }
    };
    final Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(fireWatchEventsRunnable);
      threads[i].start();
    }
    for (final Thread thread : threads) {
      thread.join();
    }
    if (concurrentModificationExceptions.get() > 0) {
      fail(concurrentModificationExceptions.get() + " ConcurrentModificationException exceptions was thrown");
    }
    if (unknownExceptions.get() > 0) {
      fail(unknownExceptions.get() + " unknown exceptions was thrown");
    }
    zkClient.close();
    mockery.assertIsSatisfied();
  }

  protected class MyListener implements IZkChildListener, IZkDataListener {

    public int _counter = 0;

    public void handleChildChange(final String parentPath, final List<String> currentChilds) throws KattaException {
      handleEvent();
    }

    public void handleDataAdded(final String dataPath, final Writable data) throws KattaException {
      handleEvent();
    }

    public void handleDataChange(final String dataPath, final Writable data) throws KattaException {
      handleEvent();
    }

    public void handleDataDeleted(final String dataPath) throws KattaException {
      handleEvent();
    }

    public Writable createWritable() {
      return new IntWritable();
    }

    private void handleEvent() {
      _counter++;
    }

  }
}
