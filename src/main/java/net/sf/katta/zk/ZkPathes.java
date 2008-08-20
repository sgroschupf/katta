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

  public static String getNodePath(String node) {
    return buildPath(NODES, node);
  }

  public static String getShard2NodeRootPath(String shard) {
    return buildPath(SHARD_TO_NODE, shard);
  }

  public static String getShard2NodePath(String shard, String node) {
    return buildPath(SHARD_TO_NODE, shard, node);
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
}
