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
package net.sf.katta.testutil;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.sf.katta.master.Master;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.util.FileUtil;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoAssertionError;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

public class TestUtil {

  private static final Logger LOG = Logger.getLogger(TestUtil.class);

  /**
   * This waits until the provided {@link Callable} returns an object that is
   * equals to the given expected value or the timeout has been reached. In both
   * cases this method will return the return value of the latest
   * {@link Callable} execution.
   * 
   * @param expectedValue
   *          The expected value of the callable.
   * @param callable
   *          The callable.
   * @param <T>
   *          The return type of the callable.
   * @param timeUnit
   *          The timeout timeunit.
   * @param timeout
   *          The timeout.
   * @return the return value of the latest {@link Callable} execution.
   * @throws Exception
   * @throws InterruptedException
   */
  public static <T> T waitUntil(T expectedValue, Callable<T> callable, TimeUnit timeUnit, long timeout)
          throws Exception {
    long startTime = System.currentTimeMillis();
    do {
      T actual = callable.call();
      if (expectedValue.equals(actual)) {
        return actual;
      }
      if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
        return actual;
      }
      Thread.sleep(50);
    } while (true);
  }

  /**
   * This waits until a mockito verification passed (which is provided in the
   * runnable). This waits until the virification passed or the timeout has been
   * reached. If the timeout has been reached this method will rethrow the
   * {@link MockitoAssertionError} that comes from the mockito verification
   * code.
   * 
   * @param runnable
   *          The runnable containing the mockito verification.
   * @param timeUnit
   *          The timeout timeunit.
   * @param timeout
   *          The timeout.
   * @throws InterruptedException
   */
  public static void waitUntilVerified(Runnable runnable, TimeUnit timeUnit, int timeout) throws InterruptedException {
    LOG.debug("Waiting for " + timeout + " " + timeUnit + " until verified.");
    long startTime = System.currentTimeMillis();
    do {
      MockitoAssertionError exception = null;
      try {
        runnable.run();
      } catch (MockitoAssertionError e) {
        exception = e;
      }
      if (exception == null) {
        return;
      }
      if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
        LOG.debug("Timeout reached without satifying expectations.");
        throw exception;
      }
      Thread.sleep(50);
    } while (true);
  }

  public static void waitUntilNoExceptionThrown(Runnable runnable, TimeUnit timeUnit, int timeout)
          throws InterruptedException {
    long startTime = System.currentTimeMillis();
    do {
      RuntimeException exception = null;
      try {
        runnable.run();
      } catch (RuntimeException e) {
        exception = e;
      }
      if (exception == null) {
        return;
      }
      if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
        throw exception;
      }
      Thread.sleep(50);
    } while (true);
  }

  /**
   * Creates a Mockito answer object that can be used for asynchronously
   * stubbing. For example:
   * 
   * <pre>
   * final CountDownLatch countDownLatch = new CountDownLatch(1);
   * Mockito.doAnswer(TestUtil.createCountDownAnswer(countDownLatch)).when(listener).announceNode(nodeName);
   * mock.someMethod(someValue);
   * countDownLatch.await(10, TimeUnit.SECONDS);
   * Assert.assertEquals(&quot;expecting invocation within 10 seconds&quot;, 0, countDownLatch.getCount());
   * </pre>
   */

  public static Stubber createCountDownAnswer(final CountDownLatch countDownLatch) {
    return Mockito.doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) throws Throwable {
        countDownLatch.countDown();
        return null;
      }
    });
  }

  public static ZkServer startZkServer(String testName, int port) {
    return startZkServer(testName, port, ZkServer.DEFAULT_TICK_TIME);
  }

  public static ZkServer startZkServer(String testName, int port, int tickTime) {
    String dataPath = "./build/test/" + testName + "/data";
    String logPath = "./build/test/" + testName + "/log";
    FileUtil.deleteFolder(new File(dataPath));
    FileUtil.deleteFolder(new File(logPath));
    ZkServer zkServer = new ZkServer(dataPath, logPath, Mockito.mock(IDefaultNameSpace.class), port, tickTime);
    zkServer.start();
    return zkServer;
  }

  public static void waitOnLeaveSafeMode(final Master master) throws Exception {
    waitUntil(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return master.isInSafeMode();
      }
    }, TimeUnit.SECONDS, 30);
    assertEquals(false, master.isInSafeMode());
  }

  public static void waitUntilBecomeMaster(final Master master) throws Exception {
    waitUntil(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return !master.isMaster();
      }
    }, TimeUnit.SECONDS, 30);
    assertEquals(true, master.isMaster());
  }

  public static void waitUntilIndexDeployed(final InteractionProtocol protocol, final String indexName)
          throws Exception {
    waitUntil(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return protocol.getIndexMD(indexName) == null;
      }
    }, TimeUnit.SECONDS, 30);
  }

  public static void waitUntilShardsUndeployed(final InteractionProtocol protocol, final IndexMetaData indexMD)
          throws Exception {
    TestUtil.waitUntil(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        int nodeCount = 0;
        Set<Shard> shards = indexMD.getShards();
        for (Shard shard : shards) {
          nodeCount += protocol.getShardNodes(shard.getName()).size();
        }
        return nodeCount != 0;
      }
    }, TimeUnit.SECONDS, 30);
  }

  public static void waitUntilNodeServesShards(final InteractionProtocol protocol, final String nodeName,
          final int shardCount) throws Exception {
    waitUntil(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return protocol.getNodeShards(nodeName).size() == shardCount;
      }
    }, TimeUnit.SECONDS, 30);
    assertEquals(shardCount, protocol.getNodeShards(nodeName).size());
  }

}
