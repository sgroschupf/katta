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
package net.sf.katta.zk;

/**
 * Defines pathes and construction rules that are used for zookeeper
 * coordiantion.
 */
public class ZkPathes {

  private static final char SEPERATOR = '/';

  public static final String ROOT_PATH = "/katta";

  public static final String MASTER = ROOT_PATH + "/master";

  public static final String NODES = ROOT_PATH + "/nodes";

  public static final String INDEXES = ROOT_PATH + "/indexes";

  public static final String NODE_TO_SHARD = ROOT_PATH + "/node-to-shard";

  public static final String SHARD_TO_NODE = ROOT_PATH + "/shard-to-node";
  public static final String SHARD_TO_ERROR = ROOT_PATH + "/shard-to-error";

  public static String getNodePath(String node) {
    return buildPath(NODES, node);
  }

  public static String getIndexPath(String index) {
    return buildPath(INDEXES, index);
  }

  public static String getShardPath(String index, String shard) {
    return buildPath(INDEXES, index, shard);
  }

  public static String getShard2NodeRootPath(String shard) {
    return buildPath(SHARD_TO_NODE, shard);
  }

  public static String getShard2NodePath(String shard, String node) {
    return buildPath(SHARD_TO_NODE, shard, node);
  }

  public static String getShard2ErrorRootPath(String shard) {
    return buildPath(SHARD_TO_ERROR, shard);
  }

  public static String getShard2ErrorPath(String shard, String node) {
    return buildPath(SHARD_TO_ERROR, shard, node);
  }

  public static String getNode2ShardRootPath(String node) {
    return buildPath(NODE_TO_SHARD, node);
  }

  public static String getNode2ShardPath(String node, String shard) {
    return buildPath(NODE_TO_SHARD, node, shard);
  }

  private static String buildPath(String... folders) {
    StringBuilder builder = new StringBuilder();
    for (String folder : folders) {
      builder.append(folder);
      builder.append(SEPERATOR);
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  public static String getName(String path) {
    return path.substring(path.lastIndexOf(SEPERATOR) + 1);
  }
}
