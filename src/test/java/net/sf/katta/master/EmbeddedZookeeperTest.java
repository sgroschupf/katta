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
package net.sf.katta.master;

import junit.framework.TestCase;
import net.sf.katta.Katta;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

public class EmbeddedZookeeperTest extends TestCase {
  public void testEmbeddedZK() throws Exception {
    // by default there need to be a zkserver
    try {

      final ZkConfiguration conf = new ZkConfiguration();

      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            Katta.startMaster(conf);
          } catch (KattaException e) {
            e.printStackTrace();
          }
        }
      };
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.start();
      ZKClient client = new ZKClient(conf);
      client.start(10000);
      client.close();
    } finally {
      // TODO sg: the way we access zookeeper here is almost painful, but I had
      // not other idea, I guess we need to clean this up in a 2.0 version.
      if (Katta._zkServer != null) {
        Katta._zkServer.shutdown();
      }
    }
  }

  public void testNoEmbeddedZK() throws Exception {
    final ZkConfiguration conf = new ZkConfiguration();
    conf.setEmbedded(false);
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          Katta.startMaster(conf);
          fail("master start should fail, since we expect no zkserver");
        } catch (KattaException e) {
          // 
        }
      }
    };
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.start();
    try {
      new ZKClient(conf).start(5000);
      fail("this should fail, since we expect no zkserver");
    } catch (Exception e) {
    }
  }

}
