package net.sf.katta.zk;

import java.io.File;

import junit.framework.TestCase;

public class ZkPathesTest extends TestCase {

  String node1 = "node1:20000";
  String node2 = "node2:20000";
  String shard1 = "index1_1";
  String shard2 = "index1_2";

  public void testGetNodePath() throws Exception {
    assertEquals(ZkPathes.NODES + "/" + node1, ZkPathes.getNodePath(node1));
    assertFalse(ZkPathes.getNodePath(node1).equals(ZkPathes.getNodePath(node2)));
  }

  public void testGetShard2NodePath() throws Exception {
    assertEquals(ZkPathes.SHARD_TO_NODE + "/" + shard1 + "/" + node1, ZkPathes.getShard2NodePath(shard1, node1));
    assertEquals(ZkPathes.SHARD_TO_NODE + "/" + shard1 + "/" + node2, ZkPathes.getShard2NodePath(shard1, node2));
    assertEquals(ZkPathes.SHARD_TO_NODE + "/" + shard2 + "/" + node1, ZkPathes.getShard2NodePath(shard2, node1));

    assertEquals(new File(ZkPathes.getShard2NodeRootPath(shard1)).getAbsolutePath(), new File(ZkPathes
        .getShard2NodePath(shard1, node1)).getParentFile().getAbsolutePath());
  }

  public void testGetNode2ShardPath() throws Exception {
    assertEquals(ZkPathes.NODE_TO_SHARD + "/" + node1 + "/" + shard1, ZkPathes.getNode2ShardPath(node1, shard1));
    assertEquals(ZkPathes.NODE_TO_SHARD + "/" + node1 + "/" + shard2, ZkPathes.getNode2ShardPath(node1, shard2));
    assertEquals(ZkPathes.NODE_TO_SHARD + "/" + node2 + "/" + shard1, ZkPathes.getNode2ShardPath(node2, shard1));

    assertEquals(new File(ZkPathes.getNode2ShardRootPath(node1)).getAbsolutePath(), new File(ZkPathes
        .getNode2ShardPath(node1, shard1)).getParentFile().getAbsolutePath());
    assertFalse(ZkPathes.getNode2ShardPath(node1, shard1).equals(ZkPathes.getShard2NodePath(shard1, node1)));
  }
}
