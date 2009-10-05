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

public class ZkConfiguration extends KattaConfiguration {

  public static final String KATTA_PROPERTY_NAME = "katta.zk.propertyName";

  public static final String ZOOKEEPER_EMBEDDED = "zookeeper.embedded";

  public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";

  public static final String ZOOKEEPER_TIMEOUT = "zookeeper.timeout";

  public static final String ZOOKEEPER_TICK_TIME = "zookeeper.tick-time";

  public static final String ZOOKEEPER_INIT_LIMIT = "zookeeper.init-limit";

  public static final String ZOOKEEPER_SYNC_LIMIT = "zookeeper.sync-limit";

  public static final String ZOOKEEPER_DATA_DIR = "zookeeper.data-dir";

  public static final String ZOOKEEPER_LOG_DATA_DIR = "zookeeper.log-data-dir";

  public static final String ZOOKEEPER_CLIENT_PORT = "zookeeper.clientPort";

  public static final String ZOOKEEPER_ROOT_PATH = "zookeeper.root-path";

  public static final String ZK_PATH_SEPERATOR = "zookeeper.path-separator";

  public ZkConfiguration() {
    super(System.getProperty(KATTA_PROPERTY_NAME, "/katta.zk.properties"));
  }

  public ZkConfiguration(final String path) {
    super(path);
  }

  public ZkConfiguration(final File file) {
    super(file);
  }

  public boolean isEmbedded() {
    String property = getProperty(ZOOKEEPER_EMBEDDED);
    if (property == null) {
      throw new IllegalArgumentException("Could not find property " + ZOOKEEPER_EMBEDDED);
    }
    return "true".equalsIgnoreCase(property);
  }

  public void setEmbedded(boolean embeddedZk) {
    setProperty(ZOOKEEPER_EMBEDDED, "" + embeddedZk);
  }

  public ZkConfiguration(Properties properties, String filePath) {
    super(properties, filePath);
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

  public String getZKDataDir() {
    return getProperty(ZOOKEEPER_DATA_DIR);
  }
  
  public String getZKDataLogDir() {
    return getProperty(ZOOKEEPER_LOG_DATA_DIR);
  }

  public int getZKClientPort() {
    return getInt(ZOOKEEPER_CLIENT_PORT);
  }

  /*
   * The following methods have to do Zookeeper paths. These setting used to be
   * static fields of ZkPaths.
   */

  private Character sep;

  public char getSeparator() {
    if (sep == null) {
      String s = getProperty(ZK_PATH_SEPERATOR, "/");
      sep = new Character(s.length() > 0 ? s.charAt(0) : '/');
    }
    return sep.charValue();
  }

  public static final String DEFAULT_ROOT_PATH = "/katta";
  private static final String MASTER = "master";
  private static final String NODES = "nodes";
  private static final String INDEXES = "indexes";
  private static final String NODE_TO_SHARD = "node-to-shard";
  private static final String SHARD_TO_NODE = "shard-to-node";
  private static final String SHARD_TO_ERROR = "shard-to-error";
  private static final String LOADTEST_NODES = "loadtest-nodes";
  private static final String SERVER_METRICS = "server-metrics";

  private String _rootPath;

  /**
   * Look up the path of the root node to use. This is an optional setting.
   * Returns null if not found.
   * 
   * @return The root path, or null if not found.
   */
  public String getZKRootPath() {
    if (_rootPath == null) {
      _rootPath = getProperty(ZOOKEEPER_ROOT_PATH, DEFAULT_ROOT_PATH).trim();
      if (_rootPath.endsWith("/")) {
        _rootPath = _rootPath.substring(0, _rootPath.length() - 1);
      }
      if (!_rootPath.startsWith("/")) {
        _rootPath = "/" + _rootPath;
      }
    }
    return _rootPath;
  }

  public void setZKRootPath(String rootPath) {
    setProperty(ZOOKEEPER_ROOT_PATH, rootPath != null ? rootPath : DEFAULT_ROOT_PATH);
    _rootPath = null;
  }

  public String getZKMasterPath() {
    return buildPath(getZKRootPath(), MASTER);
  }

  public String getZKNodesPath() {
    return buildPath(getZKRootPath(), NODES);
  }

  public String getZKNodePath(String node) {
    return buildPath(getZKRootPath(), NODES, node);
  }

  public String getZKIndicesPath() {
    return buildPath(getZKRootPath(), INDEXES);
  }

  public String getZKIndexPath(String index) {
    return buildPath(getZKRootPath(), INDEXES, index);
  }

  public String getZKShardPath(String index, String shard) {
    return buildPath(getZKRootPath(), INDEXES, index, shard);
  }

  public String getZKNodeToShardPath() {
    return buildPath(getZKRootPath(), NODE_TO_SHARD);
  }

  public String getZKNodeToShardPath(String node) {
    return buildPath(getZKRootPath(), NODE_TO_SHARD, node);
  }

  public String getZKNodeToShardPath(String node, String shard) {
    return buildPath(getZKRootPath(), NODE_TO_SHARD, node, shard);
  }

  public String getZKShardToNodePath() {
    return buildPath(getZKRootPath(), SHARD_TO_NODE);
  }

  public String getZKShardToNodePath(String shard) {
    return buildPath(getZKRootPath(), SHARD_TO_NODE, shard);
  }

  public String getZKShardToNodePath(String shard, String node) {
    return buildPath(getZKRootPath(), SHARD_TO_NODE, shard, node);
  }

  public String getZKShardToErrorPath() {
    return buildPath(getZKRootPath(), SHARD_TO_ERROR);
  }

  public String getZKShardToErrorPath(String shard) {
    return buildPath(getZKRootPath(), SHARD_TO_ERROR, shard);
  }

  public String getZKShardToErrorPath(String shard, String node) {
    return buildPath(getZKRootPath(), SHARD_TO_ERROR, shard, node);
  }

  public String getZKLoadTestPath() {
    return buildPath(getZKRootPath(), LOADTEST_NODES);
  }

  private String buildPath(String... folders) {
    StringBuilder builder = new StringBuilder();
    char sep = getSeparator();
    for (String folder : folders) {
      builder.append(folder);
      builder.append(sep);
    }
    if (builder.length() > 0) {
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  public String getZKName(String path) {
    return path.substring(path.lastIndexOf(getSeparator()) + 1);
  }

  public String getZKMetricsPathForServer(String serverId) {
    return buildPath(getZKRootPath(), SERVER_METRICS, serverId);
  }

  public String getZKMetricsPath() {
    return buildPath(getZKRootPath(), SERVER_METRICS);
  }


}
