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

import java.io.File;

public class ZkConfiguration extends KattaConfiguration {

  public static final String ZOOKEEPER_EMBEDDED = "zookeeper.embedded";
  
  public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";

  public static final String ZOOKEEPER_TIMEOUT = "zookeeper.timeout";

  public static final String ZOOKEEPER_TICK_TIME = "zookeeper.tick-time";

  public static final String ZOOKEEPER_INIT_LIMIT = "zookeeper.init-limit";

  public static final String ZOOKEEPER_SYNC_LIMIT = "zookeeper.sync-limit";

  public static final String ZOOKEEPER_DATA_DIR = "zookeeper.data-dir";

  public static final String ZOOKEEPER_LOG_DATA_DIR = "zookeeper.log-data-dir";

  public static final String ZOOKEEPER_CLIENT_PORT = "zookeeper.clientPort";

  public ZkConfiguration() {
    super("/katta.zk.properties");
  }

  public ZkConfiguration(final String path) {
    super(path);
  }

  public ZkConfiguration(final File file) {
    super(file);
  }

  public boolean isEmbedded(){
    String property = getProperty(ZOOKEEPER_EMBEDDED);
    if(property==null){
      throw new IllegalArgumentException("Could not find property "+ZOOKEEPER_EMBEDDED);
    }
    return "true".equalsIgnoreCase(property);
  }
  public void setEmbedded(boolean embeddedZk){
    setProperty(ZOOKEEPER_EMBEDDED, ""+embeddedZk);
  }
  
  public String getZKServers() {
    return getProperty(ZOOKEEPER_SERVERS);
  }

  public void setZKServers(String servers) {
    setProperty(ZOOKEEPER_SERVERS, servers);
  }

  public int getZKTimeOut() {
    return getInt(ZOOKEEPER_TIMEOUT);
  }

  public int getZKTickTime() {
    return getInt(ZOOKEEPER_TICK_TIME);
  }

  public int getZKInitLimit() {
    return getInt(ZOOKEEPER_INIT_LIMIT);
  }

  public int getZKSyncLimit() {
    return getInt(ZOOKEEPER_SYNC_LIMIT);
  }

  public File getZKDataDir() {
    return getFile(ZOOKEEPER_DATA_DIR);
  }

  public File getZKDataLogDir() {
    return getFile(ZOOKEEPER_LOG_DATA_DIR);
  }

  public int getZKClientPort() {
    return getInt(ZOOKEEPER_CLIENT_PORT);
  }
}
