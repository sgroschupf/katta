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
package net.sf.katta.util;

import net.sf.katta.DefaultNameSpaceImpl;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class ZkKattaUtil {

  public static final int DEFAULT_PORT = 2181;

  public static ZkClient startZkClient(ZkConfiguration conf, int connectionTimeout) {
    return new ZkClient(conf.getZKServers(), conf.getZKTimeOut(), connectionTimeout);
  }

  public static ZkServer startZkServer(ZkConfiguration conf) {
    return startZkServer(conf, DEFAULT_PORT);
  }

  public static ZkServer startZkServer(ZkConfiguration conf, int port) {
    ZkServer zkServer = new ZkServer(conf.getZKDataDir(), conf.getZKDataLogDir(), new DefaultNameSpaceImpl(conf), port,
            conf.getZKTickTime());
    zkServer.start();
    return zkServer;
  }
}
