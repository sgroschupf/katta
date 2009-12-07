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
package net.sf.katta.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.Katta;
import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.ShardError;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.master.Master;
import net.sf.katta.master.MasterMetaData;
import net.sf.katta.monitor.MetricsRecord;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.One2ManyListMap;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Abstracts the interaction between master and nodes via zookeeper files and
 * folders.
 * 
 * TODO use in {@link Katta}
 */
public class InteractionProtocol {

  protected final static Logger LOG = Logger.getLogger(InteractionProtocol.class);

  protected final ZkClient _zkClient;
  protected ZkConfiguration _zkConf;
  protected final List<IndexStateListener> _indexStateListeners = new CopyOnWriteArrayList<IndexStateListener>();
  protected One2ManyListMap<ConnectedComponent, Object> _zkListenerByComponent = new One2ManyListMap<ConnectedComponent, Object>();
  private One2ManyListMap<ConnectedComponent, String> _zkEphemeralPublishesByComponent = new One2ManyListMap<ConnectedComponent, String>();
  private Map<Object, String> _zkPathesByListener = new HashMap<Object, String>();
  protected volatile boolean _connected = true;

  private IZkStateListener _stateListener = new IZkStateListener() {
    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
      Set<ConnectedComponent> components = _zkListenerByComponent.keySet();
      switch (state) {
      case Disconnected:
      case Expired:
        if (_connected) { // disconnected & expired can come after each other

          // sg: in case we shutdown this thread we need to make sure all index
          // listener unsubscribe zookeeper notification to make sure zookeeper
          // is able to shutdown cleanly
          for (final IndexStateListener listener : _indexStateListeners) {
            listener.unsubscribeShardEvents();
          }
          for (ConnectedComponent component : components) {
            component.disconnect();
          }
          _connected = false;
        }
        break;
      case SyncConnected:
        _connected = true;
        for (ConnectedComponent kattaComponent : components) {
          kattaComponent.reconnect();
        }
        break;
      default:
        throw new IllegalStateException("state " + state + " not handled");
      }
    }

    @Override
    public void handleNewSession() throws Exception {
      // should be covered by handleStateChanged()
    }
  };

  public InteractionProtocol(ZkClient zkClient, ZkConfiguration zkConfiguration) {
    _zkClient = zkClient;
    _zkConf = zkConfiguration;
    _zkConf = zkConfiguration;
    LOG.info("Using ZK root path: " + _zkConf.getZKRootPath());
    _zkClient.subscribeStateChanges(_stateListener);
    new DefaultNameSpaceImpl(_zkConf).createDefaultNameSpace(_zkClient);
  }

  public void registerComponent(ConnectedComponent connectedComponent) {
    if (_zkListenerByComponent.size() == 0) {
      _zkClient.subscribeStateChanges(_stateListener);
    }
    _zkListenerByComponent.add(connectedComponent);
  }

  public void unregisterComponent(ConnectedComponent component) {
    List<Object> listeners = _zkListenerByComponent.removeKey(component);
    for (Object listener : listeners) {
      String zkPath = _zkPathesByListener.get(listener);
      if (listener instanceof IZkChildListener) {
        _zkClient.unsubscribeChildChanges(zkPath, (IZkChildListener) listener);
      } else if (listener instanceof IZkDataListener) {
        _zkClient.unsubscribeDataChanges(zkPath, (IZkDataListener) listener);
      } else {
        throw new IllegalStateException("could not handle lister of type " + listener.getClass().getName());
      }
    }

    // we deleting the ephemeral's since this is the fastest and the safest
    // way, but if this does not work, it shouldn't be too bad
    List<String> zkPublishes = _zkEphemeralPublishesByComponent.removeKey(component);
    for (String zkPath : zkPublishes) {
      _zkClient.delete(zkPath);
    }

    if (_zkListenerByComponent.size() == 0) {
      _zkClient.unsubscribeStateChanges(_stateListener);
    }
  }

  public void disconnect() {
    // TODO check if all components are unregistered ?
    _zkClient.close();
  }

  public List<String> registerNodeListener(final ConnectedComponent component, final IAddRemoveListener nodeListener) {
    return registerAddRemoveListener(component, nodeListener, _zkConf.getZKNodesPath());
  }

  public List<String> registerIndexListener(final ConnectedComponent component, final IAddRemoveListener indexListener) {
    return registerAddRemoveListener(component, indexListener, _zkConf.getZKIndicesPath());
  }

  public List<String> registerShardListener(ConnectedComponent component, String nodeName,
          final IAddRemoveListener shardListener) {
    return registerAddRemoveListener(component, shardListener, _zkConf.getZKNodeToShardPath(nodeName));
  }

  private List<String> registerAddRemoveListener(final ConnectedComponent component, final IAddRemoveListener listener,
          String zkPath) {
    synchronized (component) {
      AddRemoveListenerAdapter zkListener = new AddRemoveListenerAdapter(_zkClient, zkPath, listener);
      _zkPathesByListener.put(zkListener, zkPath);
      _zkListenerByComponent.add(component, zkListener);
      return zkListener.getCachedChilds();
    }
  }

  public void registerIndexMetaDataListener(ConnectedComponent component, String indexName,
          IZkDataListener zkDataListener) {
    String zkPath = _zkConf.getZKIndexPath(indexName);
    _zkClient.subscribeDataChanges(zkPath, zkDataListener);
    _zkPathesByListener.put(zkDataListener, zkPath);
    _zkListenerByComponent.add(component, zkDataListener);
  }

  public boolean becomeMasterOrSecondaryMaster(final Master master) {
    String masterName = master.getMasterName();
    cleanupOldMasterData(masterName);

    boolean isMaster;
    String zkMasterPath = _zkConf.getZKMasterPath();
    if (!_zkClient.exists(zkMasterPath)) {
      LOG.info(masterName + " starting as master...");
      _zkClient.createEphemeral(zkMasterPath, new MasterMetaData(masterName, System.currentTimeMillis()));
      _zkEphemeralPublishesByComponent.add(master, zkMasterPath);
      isMaster = true;
    } else {
      LOG.info(masterName + " starting as secondary master...");
      _zkClient.subscribeDataChanges(zkMasterPath, new IZkDataListener() {
        public void handleDataDeleted(final String dataPath) throws KattaException {
          master.handleMasterDisappearedEvent();
        }

        @Override
        public void handleDataChange(String dataPath, Serializable data) throws Exception {
          // do nothing
        }
      });
      isMaster = false;
    }
    return isMaster;
  }

  private void cleanupOldMasterData(final String masterName) {
    if (_zkClient.exists(_zkConf.getZKMasterPath())) {
      final MasterMetaData existingMaster = _zkClient.readData(_zkConf.getZKMasterPath());
      if (existingMaster.getMasterName().equals(masterName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(_zkConf.getZKMasterPath());
      }
    }
  }

  public List<String> getKnownNodes() {
    return _zkClient.getChildren(_zkConf.getZKNodeToShardPath());
  }

  public List<String> getNodes() {
    return _zkClient.getChildren(_zkConf.getZKNodesPath());
  }

  public List<String> getIndices() {
    return _zkClient.getChildren(_zkConf.getZKIndicesPath());
  }

  public NodeMetaData getNodeInServiceMD(String node) {
    return _zkClient.readData(_zkConf.getZKNodePath(node));
  }

  public IndexMetaData getIndexMD(String index) {
    return _zkClient.readData(_zkConf.getZKIndexPath(index));
  }

  public List<String> getIndexShards(String index) {
    return _zkClient.getChildren(_zkConf.getZKIndexPath(index));
  }

  public List<String> getNodeShards(String nodeName) {
    String zkNode2ShardPath = _zkConf.getZKNodeToShardPath(nodeName);
    List<String> children = null;
    if (_zkClient.exists(zkNode2ShardPath)) {
      children = _zkClient.getChildren(zkNode2ShardPath);
    }
    if (children == null) {
      children = Collections.EMPTY_LIST;
    }
    return children;
  }

  public ArrayList<AssignedShard> getNodeShardsMD(String nodeName, List<String> shardNames) {
    ArrayList<AssignedShard> shardMDs = new ArrayList<AssignedShard>();
    for (String shardName : shardNames) {
      AssignedShard assignedShard = _zkClient.readData(_zkConf.getZKNodeToShardPath(nodeName, shardName));
      shardMDs.add(assignedShard);
    }
    return shardMDs;
  }

  public AssignedShard getNodeShardMD(String nodeName, String shardName) {
    return _zkClient.readData(_zkConf.getZKNodeToShardPath(nodeName, shardName));
  }

  public Map<String, List<String>> getShard2NodesMap(Collection<String> shards) {
    final Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (final String shard : shards) {
      final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard);
      if (_zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard, _zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard, Collections.<String> emptyList());
      }
    }
    return shard2NodeNames;
  }

  public Map<String, List<ShardError>> getShard2ErrorMap(Collection<String> shards) {
    final Map<String, List<ShardError>> shard2Errors = new HashMap<String, List<ShardError>>();
    for (String shard : shards) {
      final String shard2ErrorPath = _zkConf.getZKShardToErrorPath(shard);
      List<String> errorNames = _zkClient.getChildren(shard2ErrorPath);
      List<ShardError> shardErrors;
      if (errorNames.isEmpty()) {
        shardErrors = Collections.EMPTY_LIST;
      } else {
        shardErrors = new ArrayList<ShardError>();
        for (String error : errorNames) {
          shardErrors.add((ShardError) _zkClient.readData(_zkConf.getZKShardToErrorPath(shard, error)));
        }
      }
      shard2Errors.put(shard, shardErrors);
    }
    return shard2Errors;
  }

  public Map<String, List<String>> getNode2ShardsMap() {
    final Map<String, List<String>> node2ShardNames = new HashMap<String, List<String>>();
    final List<String> nodes = _zkClient.getChildren(_zkConf.getZKNodeToShardPath());
    for (final String node : nodes) {
      final String node2ShardRootPath = _zkConf.getZKNodeToShardPath(node);
      if (_zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, _zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, Collections.<String> emptyList());
      }
    }
    return node2ShardNames;
  }

  public void writeIndexMD(IndexMetaData indexMetaData) {
    String zkIndexPath = _zkConf.getZKIndexPath(indexMetaData.getName());
    _zkClient.createPersistent(zkIndexPath, indexMetaData);
  }

  public void updateIndexMD(IndexMetaData indexMetaData) {
    String zkIndexPath = _zkConf.getZKIndexPath(indexMetaData.getName());
    _zkClient.writeData(zkIndexPath, indexMetaData);
  }

  public void undeployIndex(String indexName) {
    final List<String> nodes = getKnownNodes();
    for (final String node : nodes) {
      final List<String> shards = _zkClient.getChildren(_zkConf.getZKNodeToShardPath(node));
      for (final String shard : shards) {
        final String node2ShardPath = _zkConf.getZKNodeToShardPath(node, shard);
        final AssignedShard shardWritable = _zkClient.readData(node2ShardPath);
        if (shardWritable.getIndexName().equalsIgnoreCase(indexName)) {
          _zkClient.delete(node2ShardPath);
        }
      }
    }
  }

  public void cleanupShardData(Set<String> shards) {
    for (final String shard : shards) {
      final String shard2ErrorRootPath = _zkConf.getZKShardToErrorPath(shard);
      if (_zkClient.exists(shard2ErrorRootPath)) {
        final List<String> nodesWithFailedShard = _zkClient.getChildren(shard2ErrorRootPath);
        for (final String node : nodesWithFailedShard) {
          _zkClient.delete(_zkConf.getZKShardToErrorPath(shard, node));
          _zkClient.delete(_zkConf.getZKNodeToShardPath(node, shard));
        }
      }
    }
  }

  public void createShardData(String index, Set<String> shards, final Map<String, AssignedShard> shard2AssignedShardMap) {
    for (final String shard : shards) {
      final String shardZkPath = _zkConf.getZKShardPath(index, shard);
      final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard);
      final String shard2ErrorRootPath = _zkConf.getZKShardToErrorPath(shard);
      if (!_zkClient.exists(shardZkPath)) {
        _zkClient.createPersistent(shardZkPath, shard2AssignedShardMap.get(shard));
      }
      if (!_zkClient.exists(shard2NodeRootPath)) {
        _zkClient.createPersistent(shard2NodeRootPath);
      }
      if (!_zkClient.exists(shard2ErrorRootPath)) {
        _zkClient.createPersistent(shard2ErrorRootPath);
      }
    }
  }

  public void deployShards(String node, List<String> newShards, Map<String, AssignedShard> shard2AssignedShardMap) {
    final List<String> existingShards = _zkClient.getChildren(_zkConf.getZKNodeToShardPath(node));

    // add new shards
    for (final String shard2Deploy : CollectionUtil.getListOfAdded(existingShards, newShards)) {
      final String shard2NodePath = _zkConf.getZKNodeToShardPath(node, shard2Deploy);
      _zkClient.createPersistent(shard2NodePath, shard2AssignedShardMap.get(shard2Deploy));
    }

    // remove old shards
    for (final String shard2Deploy : CollectionUtil.getListOfRemoved(existingShards, newShards)) {
      _zkClient.delete(_zkConf.getZKNodeToShardPath(node, shard2Deploy));
    }

  }

  public void keepDeployStateSynced(String index, IndexMetaData indexMD, Set<String> shards, int size) {
    final IndexStateListener indexStateListener = new IndexStateListener(index, indexMD, shards, size);
    _indexStateListeners.add(indexStateListener);
    indexStateListener.subscribeShardEvents();
  }

  public void setMetric(String nodeName, MetricsRecord metricsRecord) {
    String _metricsPath = _zkConf.getZKMetricsPathForServer(nodeName);
    try {
      _zkClient.writeData(_metricsPath, metricsRecord);
    } catch (ZkNoNodeException e) {
      _zkClient.createEphemeral(_metricsPath, new MetricsRecord(nodeName));
    } catch (Exception e) {
      // this only happens if zk is down
      LOG.debug("Can't write to zk", e);
    }
  }

  public void publishShardError(Node node, AssignedShard shard, ShardError shardError) {
    String shard2ErrorPath = _zkConf.getZKShardToErrorPath(shard.getShardName(), node.getName());
    if (_zkClient.exists(shard2ErrorPath)) {
      LOG.warn("detected old shard-to-error entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2ErrorPath);
    }
    _zkClient.createEphemeral(shard2ErrorPath, shardError);
    _zkEphemeralPublishesByComponent.add(node, shard2ErrorPath);
  }

  public void publishShard(Node node, AssignedShard shard, Map<String, String> metaData) {

    // announce that this node serves this shard now...
    String shardName = shard.getShardName();
    final String shard2NodePath = _zkConf.getZKShardToNodePath(shardName, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2NodePath);
    }

    DeployedShard deployedShard = new DeployedShard(shardName, metaData);
    _zkClient.createEphemeral(shard2NodePath, deployedShard);
    _zkEphemeralPublishesByComponent.add(node, shard2NodePath);
  }

  public void unpublishShard(Node node, String shard) {
    String shard2NodePath = _zkConf.getZKShardToNodePath(shard, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      _zkClient.delete(shard2NodePath);
    }
    _zkEphemeralPublishesByComponent.removeValue(node, shard2NodePath);
  }

  public void publishNode(Node node, NodeState nodeState) {
    String nodeName = node.getName();
    LOG.info("Announce node '" + nodeName + "'...");
    final NodeMetaData metaData = new NodeMetaData(nodeName, nodeState);
    final String nodePath = _zkConf.getZKNodePath(nodeName);
    if (_zkClient.exists(nodePath)) {
      LOG.warn("Old node path '" + nodePath + "' for this node detected, deleting it...");
      _zkClient.delete(nodePath);
    }

    final String nodeToShardPath = _zkConf.getZKNodeToShardPath(nodeName);
    try {
      _zkClient.createPersistent(nodeToShardPath);
    } catch (ZkNodeExistsException e) {
      // ignore
    }
    _zkClient.createEphemeral(nodePath, metaData);
    _zkEphemeralPublishesByComponent.add(node, nodePath);
    LOG.info("Node '" + nodeName + "' announced");
  }

  public void updateNodeStatus(String nodeName, NodeState state) {
    final String nodePath = _zkConf.getZKNodePath(nodeName);
    final NodeMetaData metaData = _zkClient.readData(nodePath);
    metaData.setState(state);
    _zkClient.writeData(nodePath, metaData);
  }

  public void updateNodeStatus(String nodeName, float queriesPerMinute) {
    final String nodePath = _zkConf.getZKNodePath(nodeName);
    if (_zkClient.exists(nodePath)) {
      NodeMetaData metaData = _zkClient.readData(nodePath);
      metaData.setQueriesPerMinute(queriesPerMinute);
      _zkClient.writeData(nodePath, metaData);
    }
  }

  public void addIndex(String indexName, String indexPath, int replicationLevel) {
    final String indexZkPath = _zkConf.getZKIndexPath(indexName);
    if (_zkClient.exists(indexZkPath)) {
      throw new IllegalArgumentException("index already exists: " + indexName);
    }
    final IndexMetaData indexMetaData = new IndexMetaData(indexName, indexPath, replicationLevel,
            IndexMetaData.IndexState.ANNOUNCED);
    _zkClient.createPersistent(indexZkPath, indexMetaData);
  }

  public void removeIndex(String indexName) {
    final String indexPath = _zkConf.getZKIndexPath(indexName);
    if (!_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index not exists: " + indexName);
    }
    _zkClient.deleteRecursive(indexPath);
  }

  public boolean indexExists(String indexName) {
    return _zkClient.exists(_zkConf.getZKIndexPath(indexName));
  }

  public List<IndexMetaData> getIndicesMDs(IndexState indexState) {
    final List<String> indexes = _zkClient.getChildren(_zkConf.getZKIndicesPath());
    final List<IndexMetaData> returnIndexes = new ArrayList<IndexMetaData>();
    for (final String index : indexes) {
      final IndexMetaData metaData = _zkClient.readData(_zkConf.getZKIndexPath(index));
      if (metaData.getState() == indexState) {
        returnIndexes.add(metaData);
      }
    }
    return returnIndexes;
  }

  protected class IndexStateListener implements IZkChildListener {

    private final Set<String> _shards;
    private final Map<String, Integer> _shardToReplicaCount = new ConcurrentHashMap<String, Integer>();
    private final Map<String, Integer> _shardToErrorCount = new ConcurrentHashMap<String, Integer>();
    private final String _index;
    private IndexMetaData _indexMetaData;
    private final int _replicationLevel;

    public IndexStateListener(final String index, final IndexMetaData indexMetaData, final Set<String> shards,
            final int nodeCount) {
      _index = index;
      _indexMetaData = indexMetaData;
      _shards = shards;

      // TODO jz: this should be part of the distributionPlan
      _replicationLevel = Math.min(_indexMetaData.getReplicationLevel(), nodeCount);
    }

    public void subscribeShardEvents() {
      _zkClient.getEventLock().lock();
      try {
        LOG.info("start watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
        final Set<String> shards = _shards;
        for (final String shard : shards) {
          final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard);
          final String shard2ErrorPath = _zkConf.getZKShardToErrorPath(shard);
          _zkClient.subscribeChildChanges(shard2NodeRootPath, this);
          _zkClient.subscribeChildChanges(shard2ErrorPath, this);
          _shardToReplicaCount.put(shard, _zkClient.getChildren(shard2NodeRootPath).size());
          _shardToErrorCount.put(shard, _zkClient.getChildren(shard2ErrorPath).size());
        }
        checkForIndexStateSwitch();
      } finally {
        _zkClient.getEventLock().unlock();
      }
    }

    public void unsubscribeShardEvents() {
      LOG.info("stop watching index '" + _index + "' (" + _indexMetaData.getState() + ")");
      final Set<String> shards = _shards;
      for (final String shard : shards) {
        final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard);
        final String shard2ErrorPath = _zkConf.getZKShardToErrorPath(shard);
        _zkClient.unsubscribeChildChanges(shard2NodeRootPath, this);
        _zkClient.unsubscribeChildChanges(shard2ErrorPath, this);
      }
      _indexStateListeners.remove(this);
    }

    public void handleChildChange(final String parentPath, List<String> currentChilds) throws KattaException {
      if (currentChilds == null) {
        currentChilds = Collections.EMPTY_LIST;
      }
      final String shard = _zkConf.getZKName(parentPath);
      if (parentPath.startsWith(_zkConf.getZKShardToNodePath())) {
        _shardToReplicaCount.put(shard, currentChilds.size());
      } else if (parentPath.startsWith(_zkConf.getZKShardToErrorPath())) {
        _shardToErrorCount.put(shard, currentChilds.size());
      } else {
        throw new IllegalStateException("could not associate path " + parentPath);
      }
      checkForIndexStateSwitch();
    }

    private synchronized void checkForIndexStateSwitch() {
      if (_indexMetaData.getState() == IndexState.DEPLOYED || _indexMetaData.getState() == IndexState.ERROR) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("check with following deployments:" + _shardToReplicaCount);
        LOG.debug("check with following errors:" + _shardToErrorCount);
      }
      int notDeployed = 0;
      int underReplicated = 0;
      int failed = 0;
      int failedCompletely = 0;

      for (final String shard : _shards) {
        final Integer replicaCount = _shardToReplicaCount.get(shard);
        if (replicaCount == null || replicaCount == 0) {
          notDeployed++;
        } else if (replicaCount < _replicationLevel) {
          underReplicated++;
        }
        final Integer errorCount = _shardToErrorCount.get(shard);
        if (errorCount != null) {
          failed += errorCount;
          if (errorCount >= _replicationLevel) {
            failedCompletely++;
          }
        }
      }

      if (failedCompletely > 0) {
        // at least one shard could not be deployed on any node
        LOG.info("index '" + _index + "' deployment failed");
        switchIndexState(IndexState.ERROR);
        unsubscribeShardEvents();
      } else if (notDeployed == 0 && underReplicated == 0 && failed == 0) {
        // all shards are fully replicated
        switchIndexState(IndexState.DEPLOYED);
        unsubscribeShardEvents();
      } else if (notDeployed == 0 && underReplicated > 0) {
        // all shards are at least 1 time deployed
        switchIndexState(IndexState.REPLICATING);
      } else if (notDeployed == 0 && underReplicated - failed == 0) {
        LOG.info("index '" + _index + "' deployed with errors");
        switchIndexState(IndexState.DEPLOYED);
        unsubscribeShardEvents();

        try {
          // TODO balance index deployment!!!
          // _updateLock.lock();
          // // we wakeup the manager thread since part of this index needs to
          // be
          // // redeployed
          // _updateLock.getUpdatedCondition().signalAll();
          // // TODO sg: if a shard is corrupted it would fail to deploy on all
          // // nodes. So in case a shard just failed once on one specific node,
          // // than we can assume that with the next distribution map it will
          // be
          // // deployed on a different node. In order to allow distribution
          // // policies to handle such cases we should pass in node-shard-error
          // // map as well
        } finally {
          // _updateLock.unlock();
        }
      }

    }

    private void switchIndexState(final IndexState indexState) {
      if (_indexMetaData.getState() == indexState) {
        return;
      }

      LOG
              .info("switching index '" + _index + "' from state " + _indexMetaData.getState() + " into state "
                      + indexState);
      final String indexZkPath = _zkConf.getZKIndexPath(_index);
      _indexMetaData = _zkClient.readData(indexZkPath);
      if (indexState == IndexState.ERROR) {
        _indexMetaData.setState(indexState, "could not deploy shards properly, please see node logs");
      } else {
        _indexMetaData.setState(indexState);
      }
      _zkClient.writeData(indexZkPath, _indexMetaData);
    }
  }

  protected static class AddRemoveListenerAdapter implements IZkChildListener {

    private List<String> _cachedChilds;
    private final IAddRemoveListener _listener;

    public AddRemoveListenerAdapter(ZkClient zkClient, String path, IAddRemoveListener listener) {
      _listener = listener;
      _cachedChilds = zkClient.subscribeChildChanges(path, this);
    }

    public List<String> getCachedChilds() {
      return _cachedChilds;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      if (currentChilds == null) {
        currentChilds = Collections.EMPTY_LIST;
      }
      List<String> addedChilds = CollectionUtil.getListOfAdded(_cachedChilds, currentChilds);
      List<String> removedChilds = CollectionUtil.getListOfRemoved(_cachedChilds, currentChilds);
      for (String addedChild : addedChilds) {
        _listener.added(addedChild);
      }
      for (String removedChild : removedChilds) {
        _listener.removed(removedChild);
      }
      _cachedChilds = currentChilds;
    }

  }

}
