package net.sf.katta.master;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class DistributeShardsThread extends Thread {

  private final static Logger LOG = Logger.getLogger(DistributeShardsThread.class);

  private static final byte NOT_DEPLOYED = 0;
  private static final byte DEPLOYED = 1;
  private static final byte DEPLOYED_WITH_ERRORS = 2;

  private final ZKClient _zkClient;
  private final IDeployPolicy _deployPolicy;

  private final Set<String> _nodes = new HashSet<String>();
  BlockingQueue<String> _indicesToDeploy = new LinkedBlockingQueue<String>();

  private Lock _nodeLock = new ReentrantLock();
  private Condition _nodeUpdatedCondition = _nodeLock.newCondition();

  public DistributeShardsThread(ZKClient zkClient, IDeployPolicy deployPolicy) {
    _deployPolicy = deployPolicy;
    _zkClient = zkClient;
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (true) {
        String index = _indicesToDeploy.take();
        try {
          final String indexZkPath = ZkPathes.getIndexPath(index);
          final IndexMetaData indexMetaData = new IndexMetaData();
          _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);

          Map<String, AssignedShard> shard2AssignedShardMap = readShardsFromFs(index, indexMetaData);
          Set<String> indexShards = shard2AssignedShardMap.keySet();
          LOG.info("Found shards '" + indexShards + "' for index '" + index + "'");

          if (indexMetaData.getState() == IndexState.ANNOUNCED) {
            LOG.info("deploying index '" + index + "'");
            indexMetaData.setState(IndexMetaData.IndexState.DEPLOYING);
            _zkClient.writeData(indexZkPath, indexMetaData);
          } else {
            LOG.info("rebalancing index '" + index + "', state:" + indexMetaData.getState());
            indexMetaData.setState(IndexMetaData.IndexState.REBALANCING);
            _zkClient.writeData(indexZkPath, indexMetaData);
          }
          waitOnNodes();
          deployOrRebalanceIndex(index, indexZkPath, indexMetaData, indexShards, shard2AssignedShardMap);
        } catch (KattaException e) {
          LOG.error("Failed to deploy index '" + index + "'.", e);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("index deploy thread stopped");
    }
  }

  private void deployOrRebalanceIndex(String index, String indexZkPath, IndexMetaData indexMD, Set<String> indexShards,
      Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {
    // add shards to zk
    for (final String shard : indexShards) {
      final String shardZkPath = ZkPathes.getShardPath(index, shard);
      final String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
      if (!_zkClient.exists(shardZkPath)) {
        _zkClient.create(shardZkPath, shard2AssignedShardMap.get(shard));
      }
      if (!_zkClient.exists(shard2NodeRootPath)) {
        _zkClient.create(shard2NodeRootPath);
      }
    }

    Map<String, List<String>> currentShard2NodesMap = readShard2NodesMapFromZk(_zkClient, indexShards);
    Map<String, List<String>> currentNodeToShardsMap = readNode2ShardsMapFromZk(_zkClient);
    final Map<String, List<String>> distributionMap = _deployPolicy.createDistributionPlan(currentShard2NodesMap,
        currentNodeToShardsMap, new ArrayList<String>(_nodes), indexMD.getReplicationLevel());
    writeShardDistributionMapToZK(distributionMap, shard2AssignedShardMap);
    LOG.info("Wait for index '" + index + "' to be deployed/rebalanced.");
    try {
      byte deployResult = NOT_DEPLOYED;
      while ((deployResult = isDeployedAsExpected(distributionMap)) == NOT_DEPLOYED) {
        try {
          LOG.info("Index '" + index + "' not yet fully deployed, waiting");
          Thread.sleep(1000);
          try {
            if (!_zkClient.exists(indexZkPath)) {
              LOG.warn("Index '" + index + "' removed before the deployment completed.");
              return;
            }
          } catch (KattaException e) {
            LOG.warn("Error on getting katta state from zookeeper. A possbile temporary connection loss so ignoring.");
          }
        } catch (final InterruptedException e) {
          LOG.error("Deployment process was interrupted", e);
          return;
        }
      }
      _zkClient.readData(indexZkPath, indexMD);
      if (deployResult == DEPLOYED_WITH_ERRORS) {
        LOG.error("deploy of index '" + index + "' failed.");
        indexMD.setState(IndexMetaData.IndexState.DEPLOY_ERROR);
      } else {
        LOG.info("Finnaly the index '" + index + "' is deployed...");
        indexMD.setState(IndexMetaData.IndexState.DEPLOYED);
      }
      _zkClient.writeData(indexZkPath, indexMD);
    } catch (final KattaException e) {
      LOG.error("deploy of index '" + index + "' failed.", e);
      indexMD.setState(IndexMetaData.IndexState.DEPLOY_ERROR);
      try {
        _zkClient.writeData(indexZkPath, indexMD);
      } catch (final KattaException ke) {
        throw new RuntimeException("Failed to write Index deployError", ke);
      }
    }
  }

  private static Map<String, List<String>> readShard2NodesMapFromZk(ZKClient zkClient, Set<String> indexShards)
      throws KattaException {
    Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (String shard : indexShards) {
      String shard2NodeRootPath = ZkPathes.getShard2NodeRootPath(shard);
      if (zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard, zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard, Collections.EMPTY_LIST);
      }
    }
    return shard2NodeNames;
  }

  private Map<String, List<String>> readNode2ShardsMapFromZk(ZKClient zkClient) throws KattaException {
    Map<String, List<String>> node2ShardNames = new HashMap<String, List<String>>();
    List<String> nodes = zkClient.getChildren(ZkPathes.NODE_TO_SHARD);
    for (String node : nodes) {
      String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(node);
      if (zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, Collections.EMPTY_LIST);
      }
    }
    return node2ShardNames;
  }

  protected byte isDeployedAsExpected(final Map<String, List<String>> distributionMap) throws KattaException {
    byte result = NOT_DEPLOYED;
    int all = 0;
    int deployed = 0;
    int notDeployed = 0;
    int error = 0;
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      if (!_nodes.contains(node)) {
        // filter dead nodes
        continue;
      }
      // which shards this node should serve..
      final List<String> shardsToServe = distributionMap.get(node);
      all += shardsToServe.size();
      for (final String expectedShard : shardsToServe) {
        // lookup who is actually serving this shard.
        String shard2NodesRootPath = ZkPathes.getShard2NodeRootPath(expectedShard);
        final List<String> servingNodes = _zkClient.getChildren(shard2NodesRootPath);
        // is the node we expect here already?
        boolean asExpected = false;
        for (final String servingNode : servingNodes) {
          if (node.equals(servingNode)) {
            asExpected = true;
            DeployedShard deployedShard = new DeployedShard();
            _zkClient.readData(shard2NodesRootPath + "/" + servingNode, deployedShard);
            if (deployedShard.hasError()) {
              LOG.warn("shard '" + deployedShard.getShardName() + "' has error: " + deployedShard.getErrorMsg());
              error++;
            } else {
              deployed++;
            }
            break;
          }
        }
        if (!asExpected) {
          // node is not yet serving shard
          notDeployed++;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Shard '" + expectedShard + "' not yet deployed on node '" + node + "'.");
          }
        }
      }

    }
    LOG.info("deploying: " + deployed + " of " + all + " deployed, pending: " + notDeployed + ", error: " + error);
    // all as expected..

    if ((deployed + error) == all) {
      if (error > 0) {
        result = DEPLOYED_WITH_ERRORS;
      } else {
        result = DEPLOYED;
      }
    }
    return result;
  }

  private void waitOnNodes() throws InterruptedException {
    _nodeLock.lock();
    while (_nodes.isEmpty()) {
      LOG.info("no nodes connected, waiting...");
      _nodeUpdatedCondition.await();
    }
    _nodeLock.unlock();
  }

  private void writeShardDistributionMapToZK(final Map<String, List<String>> distributionMap,
      Map<String, AssignedShard> shard2AssignedShardMap) throws KattaException {
    final Set<String> nodes = distributionMap.keySet();
    for (final String node : nodes) {
      List<String> existingShards = _zkClient.getChildren(ZkPathes.getNode2ShardRootPath(node));
      final List<String> newShards = distributionMap.get(node);

      // add new shards
      for (String shard2Deploy : ComparisonUtil.getNew(existingShards, newShards)) {
        final String shard2NodePath = ZkPathes.getNode2ShardPath(node, shard2Deploy);
        _zkClient.create(shard2NodePath, shard2AssignedShardMap.get(shard2Deploy));
      }

      // remove old shards
      for (String shard2Deploy : ComparisonUtil.getRemoved(existingShards, newShards)) {
        _zkClient.delete(ZkPathes.getNode2ShardPath(node, shard2Deploy));
      }
    }
  }

  public void addIndex(String indexName) {
    LOG.info("add index '" + indexName + "' to deploy queue");
    _indicesToDeploy.add(indexName);
  }

  public void rebalanceIndex(String indexName) {
    LOG.info("add index '" + indexName + "' to rebalance queue");
    _indicesToDeploy.add(indexName);
  }

  public void removeIndex(String indexName) throws KattaException {
    LOG.info("remove index '" + indexName + "'");
    boolean wasInQueue = _indicesToDeploy.remove(indexName);
    if (wasInQueue) {
      return;
    }

    // iterates through all nodes and removes the assigned shards from index
    synchronized (_zkClient.getSyncMutex()) {
      // jz: we read the node2ShardRootPathes from zk to be sure we delete it
      // from nodes which are currently disconnected
      List<String> nodes = _zkClient.getChildren(ZkPathes.NODE_TO_SHARD);
      for (String node : nodes) {
        final List<String> shards = _zkClient.getChildren(ZkPathes.getNode2ShardRootPath(node));
        for (final String shard : shards) {
          final String node2ShardPath = ZkPathes.getNode2ShardPath(node, shard);
          final AssignedShard shardWritable = new AssignedShard();
          _zkClient.readData(node2ShardPath, shardWritable);
          if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
            _zkClient.delete(node2ShardPath);
          }
        }
      }
    }
  }

  public void updateNodes(List<String> nodes) {
    LOG.info("updating nodes: " + nodes);
    _nodeLock.lock();
    _nodes.clear();
    _nodes.addAll(nodes);
    _nodeUpdatedCondition.signalAll();
    _nodeLock.unlock();
  }

  private Map<String, AssignedShard> readShardsFromFs(final String index, final IndexMetaData indexMetaData)
      throws KattaException {
    final String indexPath = indexMetaData.getPath();
    // get shard folders from source
    URI uri;
    try {
      uri = new URI(indexPath);
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException(
          "unable to parse index path uri, make sure it starts with file:// or hdfs:// ", e);
    }
    FileSystem fileSystem;
    try {
      fileSystem = FileSystem.get(uri, new Configuration());
    } catch (final IOException e) {
      throw new IllegalArgumentException(
          "unable to retrive file system, make sure your path starts with hadoop support prefix like file:// or hdfs://");
    }
    Map<String, AssignedShard> shard2AssignedShard = new HashMap<String, AssignedShard>();
    try {
      final FileStatus[] listStatus = fileSystem.listStatus(new Path(indexPath), new PathFilter() {
        public boolean accept(final Path aPath) {
          return !aPath.getName().startsWith(".");
        }
      });
      for (final FileStatus fileStatus : listStatus) {
        if (fileStatus.isDir() || fileStatus.getPath().toString().endsWith(".zip")) {
          AssignedShard assignedShard = new AssignedShard(index, fileStatus.getPath().toString());
          shard2AssignedShard.put(assignedShard.getShardName(), assignedShard);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException("unable to list index path: " + indexPath, e);
    }

    if (shard2AssignedShard.size() == 0) {
      indexMetaData.setState(IndexMetaData.IndexState.NO_VALID_KATTA_INDEX);
      _zkClient.writeData(ZkPathes.getIndexPath(index), indexMetaData);
      throw new KattaException("No shards in folder found, this is not a valid katta index.");
    }
    return shard2AssignedShard;
  }

}
