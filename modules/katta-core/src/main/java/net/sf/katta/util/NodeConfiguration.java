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
import java.util.Properties;

@SuppressWarnings("serial")
public class NodeConfiguration extends KattaConfiguration {

  private final static String NODE_SERVER_PORT_START = "node.server.port.start";
  private static final String SHARD_FOLDER = "node.shard.folder";
  private static final String SHARD_DEPLOY_THROTTLE = "node.shard.deploy.throttle";
  private static final String MONITOR_CLASS = "node.monitor.class";
  private static final String SERVER_CLASS = "node.server.class";
  private static final String RPC_HANDLER_COUNT = "node.rpc.handler-count";

  public NodeConfiguration() {
    super("/katta.node.properties");
  }

  public NodeConfiguration(File file) {
    super(file);
  }

  public NodeConfiguration(Properties properties) {
    super(properties, "n/a");
  }

  public int getStartPort() {
    return getInt(NODE_SERVER_PORT_START);
  }

  public void setStartPort(final int value) {
    setProperty(NODE_SERVER_PORT_START, value + "");
  }

  public File getShardFolder() {
    return getFile(SHARD_FOLDER);
  }

  public void setShardDeployThrottle(int deployThrottle) {
    setProperty(SHARD_DEPLOY_THROTTLE, deployThrottle);
  }

  /**
   * @return a bandwith limitation in bytes/sec for shard installation
   */
  public int getShardDeployThrottle() {
    return getInt(SHARD_DEPLOY_THROTTLE, 0);
  }

  public void setShardFolder(final String value) {
    setProperty(SHARD_FOLDER, value);
  }

  public String getMonitorClass() {
    return getProperty(MONITOR_CLASS);
  }

  public String getServerClassName() {
    return getProperty(SERVER_CLASS);
  }

  public Class<?> getServerClass() {
    return getClass(SERVER_CLASS);
  }

  public int getRpcHandlerCount() {
    return getInt(RPC_HANDLER_COUNT, 25);
  }

  public void setRpcHandlerCount(int handlerCount) {
    setProperty(RPC_HANDLER_COUNT, handlerCount);
  }

}
