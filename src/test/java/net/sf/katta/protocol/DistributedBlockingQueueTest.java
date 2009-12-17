package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DistributedBlockingQueueTest {

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
    _zkClient.createPersistent("/queue");
    final DistributedBlockingQueue<Long> distributedQueue = new DistributedBlockingQueue<Long>(_zkClient, "/queue");
    final List<Long> poppedElements = new ArrayList<Long>();
    Thread thread = new Thread() {
      public void run() {
        try {
          poppedElements.add(distributedQueue.poll());
          poppedElements.add(distributedQueue.poll());
        } catch (InterruptedException e) {
          fail(e.getMessage());
        }
      }
    };
    thread.start();
    Thread.sleep(500);
    assertTrue(thread.isAlive());
    assertEquals(0, poppedElements.size());

    distributedQueue.offer(17L);
    distributedQueue.offer(18L);
    do {
      Thread.sleep(25);
    } while (thread.isAlive());
    assertEquals(2, poppedElements.size());
    assertEquals((Long) 17L, poppedElements.get(0));
    assertEquals((Long) 18L, poppedElements.get(1));
    assertFalse(thread.isAlive());
  }
}
