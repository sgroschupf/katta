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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.master.Master;
import net.sf.katta.master.MasterMetaData;
import net.sf.katta.monitor.MetricsRecord;
import net.sf.katta.node.Node;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.ShardError;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.One2ManyListMap;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Abstracts the interaction between master and nodes via zookeeper files and
 * folders.
 * 
 * TODO explainStructure() *
 * <p>
 * ZK Structure:<br>
 * + root<br>
 * ...|+ version (katta version information)<br>
 * ...|+ master (the master ephemeral)<br>
 * ...|+ nodes<br>
 * ........|+ metadata (persistents of connected & unconnected nodes) <br>
 * ........|+ live (ephemerals of connected nodes) <br>
 * ...|+ indices<br>
 * ........|+ live (persistents of indices) <br>
 * ........|+ error (persistents of error indices) <br>
 * ...|+ work (contains intermittent data of operations)<br>
 */
public class InteractionProtocol {

  protected final static Logger LOG = Logger.getLogger(InteractionProtocol.class);

  protected volatile boolean _connected = true;
  protected final ZkClient _zkClient;
  protected ZkConfiguration _zkConf;
  protected final List<IndexStateListener> _indexStateListeners = new CopyOnWriteArrayList<IndexStateListener>();

  // we govern the various listener and ephemerals to remove burden from
  // listener-users to unregister/delete them
  protected One2ManyListMap<ConnectedComponent, Object> _zkListenerByComponent = new One2ManyListMap<ConnectedComponent, Object>();
  private One2ManyListMap<ConnectedComponent, String> _zkEphemeralPublishesByComponent = new One2ManyListMap<ConnectedComponent, String>();
  private One2ManyListMap<Object, String> _zkPathesByListener = new One2ManyListMap<Object, String>();

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
      List<String> zkPathes = _zkPathesByListener.getValues(listener);
      for (String zkPath : zkPathes) {
        if (listener instanceof IZkChildListener) {
          _zkClient.unsubscribeChildChanges(zkPath, (IZkChildListener) listener);
        } else if (listener instanceof IZkDataListener) {
          _zkClient.unsubscribeDataChanges(zkPath, (IZkDataListener) listener);
        } else {
          throw new IllegalStateException("could not handle lister of type " + listener.getClass().getName());
        }
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

  @Deprecated
  public List<String> registerShardListener(ConnectedComponent component, String nodeName,
          final IAddRemoveListener shardListener) {
    return registerAddRemoveListener(component, shardListener, _zkConf.getZKNodeToShardPath(nodeName));
  }

  public void registerNodeMetaDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    registerDataListener(component, _zkConf.getZKNodeMetaDataPath(nodeName), dataListener);
  }

  private List<String> registerAddRemoveListener(final ConnectedComponent component, final IAddRemoveListener listener,
          String zkPath) {
    synchronized (component) {
      AddRemoveListenerAdapter zkListener = new AddRemoveListenerAdapter(_zkClient, zkPath, listener);
      _zkPathesByListener.add(zkListener, zkPath);
      _zkListenerByComponent.add(component, zkListener);
      return zkListener.getCachedChilds();
    }
  }

  private void registerDataListener(ConnectedComponent component, String zkPath, IZkDataListener dataListener) {
    _zkClient.subscribeDataChanges(zkPath, dataListener);
    _zkListenerByComponent.add(component, dataListener);
    _zkPathesByListener.add(dataListener, zkPath);
  }

  private void unregisterDataListener(ConnectedComponent component, String zkPath, IZkDataListener dataListener) {
    _zkClient.unsubscribeDataChanges(zkPath, dataListener);
    _zkListenerByComponent.removeValue(component, dataListener);
    _zkPathesByListener.removeValue(dataListener, zkPath);
  }

  public void registerIndexMetaDataListener(ConnectedComponent component, String indexName, IZkDataListener dataListener) {
    registerDataListener(component, _zkConf.getOldZKIndexPath(indexName), dataListener);
  }

  public List<String> registerMetricsNodeListener(ConnectedComponent component, IAddRemoveListener dataListener) {
    return registerAddRemoveListener(component, dataListener, _zkConf.getZKMetricsPath());
  }

  public void registerMetricsDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    registerDataListener(component, _zkConf.getZKMetricsPathForServer(nodeName), dataListener);
  }

  public void unregisterMetricsDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    unregisterDataListener(component, _zkConf.getZKMetricsPathForServer(nodeName), dataListener);
  }

  public List<String> getKnownNodes() {
    return _zkClient.getChildren(_zkConf.getZKNodeMetaDatasPath());
  }

  public List<String> getNodes() {
    return _zkClient.getChildren(_zkConf.getZKNodesPath());
  }

  public List<String> getIndices() {
    return _zkClient.getChildren(_zkConf.getZKIndicesPath());
  }

  public List<String> getLiveIndices() {
    return _zkClient.getChildren(_zkConf.getZKIndexMetaDatasPath());
  }

  public NodeMetaData getNodeMD(String node) {
    return (NodeMetaData) readZkData(_zkConf.getZKNodeMetaDataPath(node));
  }

  public net.sf.katta.index.IndexMetaData getOldIndexMD(String index) {
    return (net.sf.katta.index.IndexMetaData) readZkData(_zkConf.getOldZKIndexPath(index));
  }

  public IndexMetaData getIndexMD(String index) {
    return (IndexMetaData) readZkData(_zkConf.getZKIndexPath(index));
  }

  private Serializable readZkData(String zkPath) {
    Serializable data = null;
    if (_zkClient.exists(zkPath)) {
      data = _zkClient.readData(zkPath);
    }
    return data;
  }

  public Collection<String> getNodeShards(String nodeName) {
    Set<String> shards = new HashSet<String>();
    List<String> shardNames = _zkClient.getChildren(_zkConf.getZKShardToNodePath());
    for (String shardName : shardNames) {
      List<String> nodeNames = _zkClient.getChildren(_zkConf.getZKShardToNodePath(shardName));
      if (nodeNames.contains(nodeName)) {
        shards.add(shardName);
      }
    }
    return shards;
  }

  public List<DeployedShard> getShardsMD(String shard) {
    List<DeployedShard> deployedShards = new ArrayList<DeployedShard>();
    List<String> nodeNames = _zkClient.getChildren(_zkConf.getZKShardToNodePath(shard));
    for (String nodeName : nodeNames) {
      DeployedShard deployedShard = _zkClient.readData(_zkConf.getZKShardToNodePath(shard, nodeName));
      deployedShards.add(deployedShard);
    }
    return deployedShards;
  }

  public int getShardReplication(String shard) {
    return _zkClient.countChildren(_zkConf.getZKShardToNodePath(shard));
  }

  public long getShardAnnounceTime(String node, String shard) {
    return _zkClient.getCreationTime(_zkConf.getZKShardToNodePath(shard, node));
  }

  public Map<String, List<String>> getShard2NodesMap(Collection<Shard> shards) {
    final Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (final Shard shard : shards) {
      final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard.getName());
      if (_zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard.getName(), _zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard.getName(), Collections.<String> emptyList());
      }
    }
    return shard2NodeNames;
  }

  public List<String> getShardNodes(String shard) {
    final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard);
    if (!_zkClient.exists(shard2NodeRootPath)) {
      return Collections.<String> emptyList();
    }
    return _zkClient.getChildren(shard2NodeRootPath);
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
    List<String> nodes = getNodes();
    for (final String node : nodes) {
      final String node2ShardRootPath = _zkConf.getZKNodeToShardPath(node);
      if (_zkClient.exists(node2ShardRootPath)) {
        node2ShardNames.put(node, _zkClient.getChildren(node2ShardRootPath));
      } else {
        node2ShardNames.put(node, new ArrayList<String>());
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

  public void cleanupShardData(String shard) {
    final String shard2ErrorRootPath = _zkConf.getZKShardToErrorPath(shard);
    if (_zkClient.exists(shard2ErrorRootPath)) {
      final List<String> nodesWithFailedShard = _zkClient.getChildren(shard2ErrorRootPath);
      for (final String node : nodesWithFailedShard) {
        _zkClient.delete(_zkConf.getZKShardToErrorPath(shard, node));
        _zkClient.delete(_zkConf.getZKNodeToShardPath(node, shard));
      }
    }
  }

  public void createShardData(IndexMetaData indexMD) {
    for (final Shard shard : indexMD.getShards()) {
      final String shard2NodeRootPath = _zkConf.getZKShardToNodePath(shard.getName());
      final String shard2ErrorRootPath = _zkConf.getZKShardToErrorPath(shard.getName());
      if (!_zkClient.exists(shard2NodeRootPath)) {
        _zkClient.createPersistent(shard2NodeRootPath);
      }
      if (!_zkClient.exists(shard2ErrorRootPath)) {
        _zkClient.createPersistent(shard2ErrorRootPath);
      }
    }
  }

  public void keepDeployStateSynced(String index, IndexMetaData indexMD, Set<String> shards, int size) {
    final IndexStateListener indexStateListener = new IndexStateListener(index, indexMD, shards, size);
    _indexStateListeners.add(indexStateListener);
    indexStateListener.subscribeShardEvents();
  }

  public DistributedBlockingQueue<LeaderOperation> publishMaster(final Master master) {
    String masterName = master.getMasterName();
    cleanupOldMasterData(masterName);

    boolean isMaster;
    String zkMasterPath = _zkConf.getZKLeaderPath();
    if (!_zkClient.exists(zkMasterPath)) {
      LOG.info(masterName + " starting as master...");
      createEphemeral(master, zkMasterPath, null);
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

    DistributedBlockingQueue<LeaderOperation> queue = null;
    if (isMaster) {
      String queuePath = _zkConf.getZKMasterQueuePath();
      if (!_zkClient.exists(queuePath)) {
        _zkClient.createPersistent(queuePath);
      }
      queue = new DistributedBlockingQueue<LeaderOperation>(_zkClient, queuePath);
    }

    LOG.info("master '" + master.getMasterName() + "' published");
    return queue;
  }

  private void cleanupOldMasterData(final String masterName) {
    if (_zkClient.exists(_zkConf.getZKLeaderPath())) {
      final MasterMetaData existingMaster = _zkClient.readData(_zkConf.getZKLeaderPath());
      if (existingMaster.getMasterName().equals(masterName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(_zkConf.getZKLeaderPath());
      }
    }
  }

  public DistributedBlockingQueue<NodeOperation> publishNode(Node node, NodeMetaData nodeMetaData) {
    LOG.info("publishing node '" + node.getName() + "' ...");
    final String nodePath = _zkConf.getZKNodePath(node.getName());
    final String nodeMetadataPath = _zkConf.getZKNodeMetaDataPath(node.getName());

    // write or update metadata
    if (_zkClient.exists(nodeMetadataPath)) {
      _zkClient.writeData(nodeMetadataPath, nodeMetaData);
    } else {
      _zkClient.createPersistent(nodeMetadataPath, nodeMetaData);
    }

    // create queue for incoming node operations
    String queuePath = _zkConf.getZKNodeQueuePath(node.getName());
    if (_zkClient.exists(queuePath)) {
      _zkClient.deleteRecursive(queuePath);
    }
    _zkClient.createPersistent(queuePath);
    DistributedBlockingQueue<NodeOperation> nodeQueue = new DistributedBlockingQueue<NodeOperation>(_zkClient,
            queuePath);

    // mark the node as connected
    if (_zkClient.exists(nodePath)) {
      LOG.warn("Old node ephemeral '" + nodePath + "' detected, deleting it...");
      _zkClient.delete(nodePath);
    }
    createEphemeral(node, nodePath, null);
    LOG.info("node '" + node.getName() + "' published");
    return nodeQueue;
  }

  public void publishShard(Node node, String shardName, Map<String, String> metaData) {
    // announce that this node serves this shard now...
    final String shard2NodePath = _zkConf.getZKShardToNodePath(shardName, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      _zkClient.delete(shard2NodePath);
    }

    DeployedShard deployedShard = new DeployedShard(shardName, metaData);
    createEphemeral(node, shard2NodePath, deployedShard);
  }

  public void publishShardError(Node node, String shardName, ShardError shardError) {
    String shard2ErrorPath = _zkConf.getZKShardToErrorPath(shardName, node.getName());
    if (_zkClient.exists(shard2ErrorPath)) {
      LOG.warn("detected old shard-to-error entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2ErrorPath);
    }
    createEphemeral(node, shard2ErrorPath, shardError);
  }

  public void unpublishShard(Node node, String shard) {
    String shard2NodePath = _zkConf.getZKShardToNodePath(shard, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      _zkClient.delete(shard2NodePath);
    }
    _zkEphemeralPublishesByComponent.removeValue(node, shard2NodePath);
  }

  public void setMetric(String nodeName, MetricsRecord metricsRecord) {
    String metricsPath = _zkConf.getZKMetricsPathForServer(nodeName);
    try {
      _zkClient.writeData(metricsPath, metricsRecord);
    } catch (ZkNoNodeException e) {
      // TODO put in ephemeral map ?
      _zkClient.createEphemeral(metricsPath, new MetricsRecord(nodeName));
    } catch (Exception e) {
      // this only happens if zk is down
      LOG.debug("Can't write to zk", e);
    }
  }

  public MetricsRecord getMetric(String nodeName) {
    return (MetricsRecord) readZkData(_zkConf.getZKMetricsPathForServer(nodeName));
  }

  private void createEphemeral(ConnectedComponent component, String path, Serializable content) {
    _zkClient.createEphemeral(path, content);
    _zkEphemeralPublishesByComponent.add(component, path);
  }

  public void addLeaderOperation(LeaderOperation operation) {
    String queuePath = _zkConf.getZKMasterQueuePath();
    new DistributedBlockingQueue<LeaderOperation>(_zkClient, queuePath).offer(operation);
  }

  public OperationId addNodeOperation(String nodeName, NodeOperation nodeOperation) {
    String queuePath = _zkConf.getZKNodeQueuePath(nodeName);
    String elementName = new DistributedBlockingQueue<NodeOperation>(_zkClient, queuePath).offer(nodeOperation);
    return new OperationId(nodeName, elementName);
  }

  public boolean isNodeOperationQueued(OperationId operationId) {
    String elementPath = _zkConf.getZKNodeQueueElementPath(operationId.getNodeName(), operationId.getElementName());
    return _zkClient.exists(elementPath);
  }

  public void registerNodeOperationListener(ConnectedComponent component, OperationId operationId,
          IZkDataListener dataListener) {
    String elementPath = _zkConf.getZKNodeQueueElementPath(operationId.getNodeName(), operationId.getElementName());
    registerDataListener(component, elementPath, dataListener);
  }

  public void updateNodeMD(NodeMetaData nodeMetaData) {
    _zkClient.writeData(_zkConf.getZKNodeMetaDataPath(nodeMetaData.getName()), nodeMetaData);
  }

  @Deprecated
  public void updateNodeStatus(String nodeName, NodeState state) {
    final String nodeMdPath = _zkConf.getZKNodeMetaDataPath(nodeName);
    final NodeMetaData metaData = _zkClient.readData(nodeMdPath);
    metaData.setState(state);
    _zkClient.writeData(nodeMdPath, metaData);
  }

  @Deprecated
  public void updateNodeStatus(String nodeName, float queriesPerMinute) {
    final String nodeMdPath = _zkConf.getZKNodeMetaDataPath(nodeName);
    if (_zkClient.exists(nodeMdPath)) {
      NodeMetaData metaData = _zkClient.readData(nodeMdPath);
      metaData.setQueriesPerMinute(queriesPerMinute);
      _zkClient.writeData(nodeMdPath, metaData);
    }
  }

  public void addIndex(String indexName, String indexPath, int replicationLevel) {
    final String indexZkPath = _zkConf.getOldZKIndexPath(indexName);
    if (_zkClient.exists(indexZkPath)) {
      throw new IllegalArgumentException("index already exists: " + indexName);
    }
    final IndexMetaData indexMetaData = new IndexMetaData(indexName, indexPath, replicationLevel, IndexState.ANNOUNCED);
    _zkClient.createPersistent(indexZkPath, indexMetaData);
  }

  public void removeIndex(String indexName) {
    final String indexPath = _zkConf.getOldZKIndexPath(indexName);
    if (!_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index not exists: " + indexName);
    }
    _zkClient.deleteRecursive(indexPath);
  }

  public boolean indexExists(String indexName) {
    return _zkClient.exists(_zkConf.getOldZKIndexPath(indexName));
  }

  public List<IndexMetaData> getIndicesMDs(IndexState indexState) {
    final List<String> indexes = _zkClient.getChildren(_zkConf.getZKIndicesPath());
    final List<IndexMetaData> returnIndexes = new ArrayList<IndexMetaData>();
    for (final String index : indexes) {
      IndexMetaData metaData = getIndexMD(index);
      if (metaData.getState() == indexState) {
        returnIndexes.add(metaData);
      }
    }
    return returnIndexes;
  }

  public void showStructure() {
    _zkClient.showFolders(System.out);
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
      final String indexZkPath = _zkConf.getOldZKIndexPath(_index);
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
      if (_cachedChilds == null) {
        _cachedChilds = Collections.EMPTY_LIST;
      }
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
