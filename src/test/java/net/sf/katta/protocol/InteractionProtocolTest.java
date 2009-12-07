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

import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.ZkTestSystem;

import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Rule;
import org.junit.Test;

public class InteractionProtocolTest {

  @Rule
  public ZkTestSystem _zk = ZkTestSystem.getInstance();
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  @Test(timeout = 7000)
  public void testLifecycle() throws Exception {
    int GATEWAY_PORT = 2190;
    Gateway gateway = new Gateway(GATEWAY_PORT, _zk.getServerPort());
    gateway.start();
    ZkClient zkClient = new ZkClient("localhost:" + GATEWAY_PORT);

    InteractionProtocol protocol = new InteractionProtocol(zkClient, _zk.getZkConf());
    final AtomicInteger connectCount = new AtomicInteger();
    final AtomicInteger disconnectCount = new AtomicInteger();
    final Object mutex = new Object();

    protocol.registerComponent(new ConnectedComponent() {
      @Override
      public void disconnect() {
        disconnectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }

      @Override
      public void reconnect() {
        connectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }
    });
    synchronized (mutex) {
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
    }
    zkClient.close();
    assertEquals(2, connectCount.get());
    assertEquals(2, connectCount.get());
  }
}
