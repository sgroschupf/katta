/**
 * Copyright 2009 the original author or authors.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.AbstractZkTest;

import org.junit.Test;

public class BlockingQueueTest extends AbstractZkTest {

  private String getRootPath() {
    // this path is cleaned up by ZkSystem!
    return _zk.getZkConf().getZkRootPath() + "/queue";
  }

  @Test(timeout = 15000)
  public void testBlockingPoll() throws Exception {
    final BlockingQueue<Long> queue = new BlockingQueue<Long>(_zk.getZkClient(), getRootPath());
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
    BlockingQueue<Long> queue = new BlockingQueue<Long>(_zk.getZkClient(), getRootPath());
    Long element = new Long(1);
    String elementId = queue.add(element);

    // reinitialization
    queue = new BlockingQueue<Long>(_zk.getZkClient(), getRootPath());
    assertEquals(element, queue.peek());

    String elementId2 = queue.add(element);
    assertNotSame(elementId, elementId2);
  }
}
