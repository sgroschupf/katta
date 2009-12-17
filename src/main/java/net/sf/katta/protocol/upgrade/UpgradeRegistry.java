package net.sf.katta.protocol.upgrade;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

/**
 * 
 * TODO node metadata
 * 
 * TODO index metadata
 * 
 */
public class UpgradeRegistry {

  private static final Logger LOG = Logger.getLogger(UpgradeRegistry.class);

  public static void upgradeMasterIfNecessary(InteractionProtocol protocol) {

  }

  public static void upgradeNodeIfNecessary(InteractionProtocol protocol, ZkClient zkClient,
          ZkConfiguration zkConfiguration, Node node) {
    // check version
    convertShardAssignmentInformation(_protocol, zkClient, zkConfiguration, node);
  }

  private static void convertShardAssignmentInformation(InteractionProtocol protocol, ZkClient zkClient,
          ZkConfiguration zkConfiguration, Node node) {
    LOG.info("converting shard assignment information to new structure");
    NodeMetaData nodeMetaData = new NodeMetaData(node.getName(), node.getState());
    List<String> shardsToServe = _protocol.getNodeShards(node.getName());
    LOG.info("found " + shardsToServe.size() + " shards to serve: " + shardsToServe);
    ArrayList<AssignedShard> assignedShards = _protocol.getNodeShardsMD(node.getName(), shardsToServe);
    for (AssignedShard assignedShard : assignedShards) {
      String indexName = assignedShard.getIndexName();
      nodeMetaData.addShard(indexName, assignedShard.getName(), assignedShard.getPath());
    }
    String metaDataPath = zkConfiguration.getZKNodeMetaDataPath(node.getName());
    if (zkClient.exists(metaDataPath)) {
      zkClient.delete(metaDataPath);
    }
    zkClient.createPersistent(metaDataPath, nodeMetaData);
  }
}
