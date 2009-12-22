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
package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OperationQueueTest {

  private ZkServer _zkServer;
  private ZkClient _zkClient;

  @Before
  public void setUp() {
    _zkServer = net.sf.katta.testutil.TestUtil.startZkServer("ZkClientTest-DistributedBlockingQueueTest", 4711);
    _zkClient = _zkServer.getZkClient();
  }

  @After
  public void tearDown() {
    if (_zkServer != null) {
      _zkServer.shutdown();
    }
  }

  @Test(timeout = 15000)
  public void testBlockingPoll() throws Exception {
    final OperationQueue<Long> queue = new OperationQueue<Long>(_zkClient, "/queue");
    final List<Long> poppedElements = new ArrayList<Long>();
    Thread thread = new Thread() {
      public void run() {
        try {
          poppedElements.add(queue.peek());
          queue.remove();
          poppedElements.add(queue.peek());
          queue.remove();
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
      }
    };
    thread.start();
    Thread.sleep(500);
    assertTrue(thread.isAlive());
    assertEquals(0, poppedElements.size());

    queue.add(17L);
    queue.add(18L);
    do {
      Thread.sleep(25);
    } while (thread.isAlive());
    assertEquals(2, poppedElements.size());
    assertEquals((Long) 17L, poppedElements.get(0));
    assertEquals((Long) 18L, poppedElements.get(1));
    assertFalse(thread.isAlive());
  }

  @Test(timeout = 15000)
  public void testReinitialization() throws Exception {
    OperationQueue<Long> queue = new OperationQueue<Long>(_zkClient, "/queue");
    Long element = new Long(1);
    String elementId = queue.add(element);

    // reinitialization
    queue = new OperationQueue<Long>(_zkClient, "/queue");
    assertEquals(element, queue.peek());

    String elementId2 = queue.add(element);
    assertNotSame(elementId, elementId2);
  }

  @Test(timeout = 15000)
  public void testResultMechanism() throws Exception {
    final OperationQueue<Long> queue = new OperationQueue<Long>(_zkClient, "/queue");
    Long element = new Long(1);
    String elementId = queue.add(element);

    assertEquals(element, queue.peek());
    String result = "result1";
    queue.remove(result);
    assertEquals(result, queue.getResult(elementId, false));

    assertNotNull(queue.getResult(elementId, true));
    assertNull(queue.getResult(elementId, true));
  }

  @Test(timeout = 15000)
  public void testResultMechanism_DeletingOldResults() throws Exception {
    final OperationQueue<Long> queue = new OperationQueue<Long>(_zkClient, "/queue");
    // cheat, we know the internals
    String elementId = "operation-0000000000";
    assertNull(queue.getResult(elementId, false));
    _zkClient.createPersistent("/queue/results/" + elementId, "");
    assertNotNull(queue.getResult(elementId, false));

    queue.add(new Long(1));
    assertNull(queue.getResult(elementId, false));
  }

}
