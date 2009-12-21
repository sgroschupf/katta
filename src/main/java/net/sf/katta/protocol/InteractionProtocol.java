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

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.master.Master;
import net.sf.katta.monitor.MetricsRecord;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.MasterMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.node.NodeOperation;
import net.sf.katta.protocol.operation.node.OperationResult;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.One2ManyListMap;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkConfiguration.PathDef;

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
    _connected = false;
    _zkClient.close();
  }

  public List<String> registerNodeListener(final ConnectedComponent component, final IAddRemoveListener nodeListener) {
    return registerAddRemoveListener(component, nodeListener, _zkConf.getZkPath(PathDef.NODES_LIVE));
  }

  public void registerNodeMetaDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    registerDataListener(component, dataListener, _zkConf.getZkPath(PathDef.NODES_METADATA, nodeName));
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

  private void registerDataListener(ConnectedComponent component, IZkDataListener dataListener, String zkPath) {
    _zkClient.subscribeDataChanges(zkPath, dataListener);
    _zkListenerByComponent.add(component, dataListener);
    _zkPathesByListener.add(dataListener, zkPath);
  }

  private void unregisterDataListener(ConnectedComponent component, IZkDataListener dataListener, String zkPath) {
    _zkClient.unsubscribeDataChanges(zkPath, dataListener);
    _zkListenerByComponent.removeValue(component, dataListener);
    _zkPathesByListener.removeValue(dataListener, zkPath);
  }

  public void registerIndexMetaDataListener(ConnectedComponent component, String indexName, IZkDataListener dataListener) {
    registerDataListener(component, dataListener, _zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
  }

  public List<String> registerChildListener(ConnectedComponent component, PathDef pathDef, IAddRemoveListener listener) {
    return registerAddRemoveListener(component, listener, _zkConf.getZkPath(pathDef));
  }

  public List<String> registerChildListener(ConnectedComponent component, PathDef pathDef, String childName,
          IAddRemoveListener listener) {
    return registerAddRemoveListener(component, listener, _zkConf.getZkPath(pathDef, childName));
  }

  public void registerDataListener(ConnectedComponent component, PathDef pathDef, String childName,
          IZkDataListener listener) {
    registerDataListener(component, listener, _zkConf.getZkPath(pathDef, childName));
  }

  public void unregisterDataChanges(ConnectedComponent component, String dataPath, IZkDataListener listener) {
    unregisterDataListener(component, listener, dataPath);
  }

  public List<String> registerMetricsNodeListener(ConnectedComponent component, IAddRemoveListener dataListener) {
    return registerAddRemoveListener(component, dataListener, _zkConf.getZKMetricsPath());
  }

  public void registerMetricsDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    registerDataListener(component, dataListener, _zkConf.getZKMetricsPathForServer(nodeName));
  }

  public void unregisterMetricsDataListener(ConnectedComponent component, String nodeName, IZkDataListener dataListener) {
    unregisterDataListener(component, dataListener, _zkConf.getZKMetricsPathForServer(nodeName));
  }

  public List<String> getKnownNodes() {
    return _zkClient.getChildren(_zkConf.getZkPath(PathDef.NODES_METADATA));
  }

  public List<String> getLiveNodes() {
    return _zkClient.getChildren(_zkConf.getZkPath(PathDef.NODES_LIVE));
  }

  public List<String> getIndices() {
    return _zkClient.getChildren(_zkConf.getZkPath(PathDef.INDICES_METADATA));
  }

  public NodeMetaData getNodeMD(String node) {
    return (NodeMetaData) readZkData(_zkConf.getZkPath(PathDef.NODES_METADATA, node));
  }

  public net.sf.katta.index.IndexMetaData getOldIndexMD(String index) {
    return (net.sf.katta.index.IndexMetaData) readZkData(_zkConf.getOldZKIndexPath(index));
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
    List<String> shardNames = _zkClient.getChildren(_zkConf.getZkPath(PathDef.SHARD_TO_NODES));
    for (String shardName : shardNames) {
      List<String> nodeNames = _zkClient.getChildren(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName));
      if (nodeNames.contains(nodeName)) {
        shards.add(shardName);
      }
    }
    return shards;
  }

  public List<DeployedShard> getShardsMD(String shard) {
    List<DeployedShard> deployedShards = new ArrayList<DeployedShard>();
    List<String> nodeNames = _zkClient.getChildren(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard));
    for (String nodeName : nodeNames) {
      DeployedShard deployedShard = _zkClient.readData(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard, nodeName));
      deployedShards.add(deployedShard);
    }
    return deployedShards;
  }

  public int getShardReplication(String shard) {
    return _zkClient.countChildren(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard));
  }

  public long getShardAnnounceTime(String node, String shard) {
    return _zkClient.getCreationTime(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard, node));
  }

  public Map<String, List<String>> getShard2NodesMap(Collection<Shard> shards) {
    final Map<String, List<String>> shard2NodeNames = new HashMap<String, List<String>>();
    for (final Shard shard : shards) {
      final String shard2NodeRootPath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard.getName());
      if (_zkClient.exists(shard2NodeRootPath)) {
        shard2NodeNames.put(shard.getName(), _zkClient.getChildren(shard2NodeRootPath));
      } else {
        shard2NodeNames.put(shard.getName(), Collections.<String> emptyList());
      }
    }
    return shard2NodeNames;
  }

  public List<String> getShardNodes(String shard) {
    final String shard2NodeRootPath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard);
    if (!_zkClient.exists(shard2NodeRootPath)) {
      return Collections.<String> emptyList();
    }
    return _zkClient.getChildren(shard2NodeRootPath);
  }

  public OperationQueue<LeaderOperation> publishMaster(final Master master) {
    String masterName = master.getMasterName();
    String zkMasterPath = _zkConf.getZkPath(PathDef.MASTER);
    cleanupOldMasterData(masterName, zkMasterPath);

    boolean isMaster;
    if (!_zkClient.exists(zkMasterPath)) {
      LOG.info(masterName + " starting as master...");
      createEphemeral(master, zkMasterPath, null);
      isMaster = true;
    } else {
      LOG.info(masterName + " starting as secondary master...");
      registerDataListener(master, new IZkDataListener() {
        public void handleDataDeleted(final String dataPath) throws KattaException {
          master.handleMasterDisappearedEvent();
        }

        @Override
        public void handleDataChange(String dataPath, Serializable data) throws Exception {
          // do nothing
        }
      }, zkMasterPath);
      isMaster = false;
    }

    OperationQueue<LeaderOperation> queue = null;
    if (isMaster) {
      String queuePath = _zkConf.getZKMasterQueuePath();
      if (!_zkClient.exists(queuePath)) {
        _zkClient.createPersistent(queuePath);
      }
      queue = new OperationQueue<LeaderOperation>(_zkClient, queuePath);
    }

    LOG.info("master '" + master.getMasterName() + "' published");
    return queue;
  }

  private void cleanupOldMasterData(final String masterName, String zkMasterPath) {
    if (_zkClient.exists(zkMasterPath)) {
      final MasterMetaData existingMaster = _zkClient.readData(zkMasterPath);
      if (existingMaster.getMasterName().equals(masterName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(zkMasterPath);
      }
    }
  }

  public OperationQueue<NodeOperation> publishNode(Node node, NodeMetaData nodeMetaData) {
    LOG.info("publishing node '" + node.getName() + "' ...");
    final String nodePath = _zkConf.getZkPath(PathDef.NODES_LIVE, node.getName());
    final String nodeMetadataPath = _zkConf.getZkPath(PathDef.NODES_METADATA, node.getName());

    // write or update metadata
    if (_zkClient.exists(nodeMetadataPath)) {
      _zkClient.writeData(nodeMetadataPath, nodeMetaData);
    } else {
      _zkClient.createPersistent(nodeMetadataPath, nodeMetaData);
    }

    // create queue for incoming node operations
    String queuePath = _zkConf.getZkPath(PathDef.NODE_QUEUE, node.getName());
    // String queuePath = _zkConf.getZKNodeQueuePath(node.getName());
    if (_zkClient.exists(queuePath)) {
      _zkClient.deleteRecursive(queuePath);
    }
    OperationQueue<NodeOperation> nodeQueue = new OperationQueue<NodeOperation>(_zkClient, queuePath);

    // mark the node as connected
    if (_zkClient.exists(nodePath)) {
      LOG.warn("Old node ephemeral '" + nodePath + "' detected, deleting it...");
      _zkClient.delete(nodePath);
    }
    createEphemeral(node, nodePath, null);
    return nodeQueue;
  }

  public void publishIndex(IndexMetaData indexMD) {
    _zkClient.createPersistent(_zkConf.getZkPath(PathDef.INDICES_METADATA, indexMD.getName()), indexMD);
  }

  public void unpublishIndex(String indexName) {
    _zkClient.deleteRecursive(_zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
  }

  public IndexMetaData getIndexMD(String index) {
    return (IndexMetaData) readZkData(_zkConf.getZkPath(PathDef.INDICES_METADATA, index));
  }

  public void updateIndexMD(IndexMetaData indexMD) {
    _zkClient.writeData(_zkConf.getZkPath(PathDef.INDICES_METADATA, indexMD.getName()), indexMD);
  }

  public void publishShard(Node node, String shardName, Map<String, String> metaData) {
    // announce that this node serves this shard now...
    final String shard2NodePath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      _zkClient.delete(shard2NodePath);
    }

    DeployedShard deployedShard = new DeployedShard(shardName, metaData);
    _zkClient.createPersistent(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName), true);
    createEphemeral(node, shard2NodePath, deployedShard);
  }

  public void unpublishShard(Node node, String shard) {
    String shard2NodePath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard, node.getName());
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
    new OperationQueue<LeaderOperation>(_zkClient, queuePath).add(operation);
  }

  public OperationId addNodeOperation(String nodeName, NodeOperation nodeOperation) {
    String elementName = getNodeQueue(nodeName).add(nodeOperation);
    return new OperationId(nodeName, elementName);
  }

  public OperationResult getNodeOperationResult(OperationId operationId, boolean remove) {
    return (OperationResult) getNodeQueue(operationId.getNodeName()).getResult(operationId.getElementName(), remove);
  }

  public boolean isNodeOperationQueued(OperationId operationId) {
    return getNodeQueue(operationId.getNodeName()).containsElement(operationId.getElementName());
  }

  public void registerNodeOperationListener(ConnectedComponent component, OperationId operationId,
          IZkDataListener dataListener) {
    String elementPath = getNodeQueue(operationId.getNodeName()).getElementPath(operationId.getElementName());
    registerDataListener(component, dataListener, elementPath);
  }

  private OperationQueue<NodeOperation> getNodeQueue(String nodeName) {
    String queuePath = _zkConf.getZkPath(PathDef.NODE_QUEUE, nodeName);
    OperationQueue<NodeOperation> nodeQueue = new OperationQueue<NodeOperation>(_zkClient, queuePath);
    return nodeQueue;
  }

  public void updateNodeMD(NodeMetaData nodeMetaData) {
    _zkClient.writeData(_zkConf.getZkPath(PathDef.NODES_METADATA, nodeMetaData.getName()), nodeMetaData);
  }

  @Deprecated
  public void updateNodeStatus(String nodeName, float queriesPerMinute) {
    final String nodeMdPath = _zkConf.getZkPath(PathDef.NODES_METADATA, nodeName);
    if (_zkClient.exists(nodeMdPath)) {
      NodeMetaData metaData = _zkClient.readData(nodeMdPath);
      metaData.setQueriesPerMinute(queriesPerMinute);
      _zkClient.writeData(nodeMdPath, metaData);
    }
  }

  public boolean indexExists(String indexName) {
    return _zkClient.exists(_zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
  }

  public void showStructure() {
    _zkClient.showFolders(System.out);
  }

  protected class AddRemoveListenerAdapter implements IZkChildListener {

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
