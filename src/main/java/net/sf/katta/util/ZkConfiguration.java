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

  private static final long serialVersionUID = 1L;

  public static final String KATTA_PROPERTY_NAME = "katta.zk.propertyName";

  public static final String ZOOKEEPER_EMBEDDED = "zookeeper.embedded";
  public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
  public static final String ZOOKEEPER_TIMEOUT = "zookeeper.timeout";
  public static final String ZOOKEEPER_TICK_TIME = "zookeeper.tick-time";
  public static final String ZOOKEEPER_INIT_LIMIT = "zookeeper.init-limit";
  public static final String ZOOKEEPER_SYNC_LIMIT = "zookeeper.sync-limit";
  public static final String ZOOKEEPER_DATA_DIR = "zookeeper.data-dir";
  public static final String ZOOKEEPER_LOG_DATA_DIR = "zookeeper.log-data-dir";

  public static final String ZOOKEEPER_ROOT_PATH = "zookeeper.root-path";
  public static Character ZK_PATH_SEPARATOR='/';

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

  
  public static final String DEFAULT_ROOT_PATH = "/katta";
  private static final String NODES = "nodes";
  private static final String WORK = "work";

  public String getZKNodeQueuePath(String node) {
    return buildZkPath(getZkRootPath(), WORK, node + "-queue");
  }

  public enum PathDef {
    MASTER("current master ephemeral", true, "master"), // 
    VERSION("current cluster version", true, "version"), // 
    NODES_METADATA("metadata of connected & unconnected nodes", true, NODES, "metadata"), // 
    NODES_LIVE("ephemerals of connected nodes", true, NODES, "live"), //
    NODE_METRICS("metrics information of nodes", false, NODES, "metrics"), //
    INDICES_METADATA("metadata of live & error indices", true, "indicies"), //
    SHARD_TO_NODES("ephemerals of nodes serving a shard", true, "shard-to-nodes"), //
    MASTER_QUEUE("master operations", false, WORK, "master-queue"), //
    NODE_QUEUE("node operations and results", false, WORK, "node-queues"), //
    FLAGS("custom flags", false, WORK, "flags"); //

    private final String _description;
    private final String[] _pathParts;
    private final boolean _vip;// very-important-path

    private PathDef(String description, boolean vip, String... pathParts) {
      _description = description;
      _vip = vip;
      _pathParts = pathParts;
    }

    public String getPath(char separator) {
      return buildPath(separator, _pathParts);
    }

    public String getDescription() {
      return _description;
    }

    public boolean isVip() {
      return _vip;
    }
  }

  /**
   * @param pathDef
   * @return ${katta.root}/pathDef/name1/name2/...
   */
  public String getZkPath(PathDef pathDef, String... names) {
    if (names.length == 0) {
      return buildZkPath(getZkRootPath(), pathDef.getPath(ZK_PATH_SEPARATOR));
    }
    String suffixPath = buildZkPath(names);
    return buildZkPath(getZkRootPath(), pathDef.getPath(ZK_PATH_SEPARATOR), suffixPath);
  }

  private String _rootPath;

  /**
   * Look up the path of the root node to use. This is an optional setting.
   * Returns null if not found.
   * 
   * @return The root path, or null if not found.
   */
  public String getZkRootPath() {
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

  public static String getZKName(String path) {
    return path.substring(path.lastIndexOf(ZK_PATH_SEPARATOR) + 1);
  }

  public static String getZkParent(String path) {
    if (path.length() == 1) {
      return null;
    }
    String name = getZKName(path);
    String parent = path.substring(0, path.length() - name.length() - 1);
    if (parent.equals("")) {
      return String.valueOf(ZK_PATH_SEPARATOR);
    }
    return parent;
  }

  public static String buildZkPath(String... folders) {
    return buildPath(ZK_PATH_SEPARATOR, folders);
  }

  static String buildPath(char separator, String... folders) {
    StringBuilder builder = new StringBuilder();
    for (String folder : folders) {
      builder.append(folder);
      builder.append(separator);
    }
    if (builder.length() > 0) {
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

}
