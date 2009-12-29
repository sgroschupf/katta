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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.master.Master;
import net.sf.katta.monitor.MetricsRecord;
import net.sf.katta.node.Node;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.operation.node.OperationResult;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.MasterMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.One2ManyListMap;
import net.sf.katta.util.StringUtil;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.I0Itec.zkclient.util.ZkPathUtil.PathFilter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Abstracts the interaction between master and nodes via zookeeper files and
 * folders.
 * 
 * <p>
 * For inspecting and understanding the zookeeper structure see
 * {@link #explainStructure()} and {@link #showStructure()}.
 */
public class InteractionProtocol {

  protected final static Logger LOG = Logger.getLogger(InteractionProtocol.class);

  protected volatile boolean _connected = true;
  protected final ZkClient _zkClient;
  protected final ZkConfiguration _zkConf;

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
    LOG.debug("Using ZK root path: " + _zkConf.getZkRootPath());
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
    if (_zkListenerByComponent.size() > 0) {
      LOG.warn("following components still connected:" + _zkListenerByComponent.keySet());
    }
    if (_zkEphemeralPublishesByComponent.size() > 0) {
      LOG.warn("following ephemeral still exists:" + _zkEphemeralPublishesByComponent.keySet());
    }
    _connected = false;
    _zkClient.close();
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

  public void unregisterDataChanges(ConnectedComponent component, PathDef pathDef, String childName,
          IZkDataListener listener) {
    unregisterDataListener(component, listener, _zkConf.getZkPath(pathDef, childName));
  }

  public void unregisterDataChanges(ConnectedComponent component, String dataPath, IZkDataListener listener) {
    unregisterDataListener(component, listener, dataPath);
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

  public MasterMetaData getMasterMD() {
    return (MasterMetaData) readZkData(_zkConf.getZkPath(PathDef.MASTER));
  }

  public NodeMetaData getNodeMD(String node) {
    return (NodeMetaData) readZkData(_zkConf.getZkPath(PathDef.NODES_METADATA, node));
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

  public ReplicationReport getReplicationReport(IndexMetaData indexMD) {
    int desiredReplicationCount = indexMD.getReplicationLevel();
    int minimalShardReplicationCount = indexMD.getReplicationLevel();
    int maximaShardReplicationCount = 0;

    Map<String, Integer> replicationCountByShardMap = new HashMap<String, Integer>();
    final Set<Shard> shards = indexMD.getShards();
    for (final Shard shard : shards) {
      final int servingNodesCount = getShardNodes(shard.getName()).size();
      replicationCountByShardMap.put(shard.getName(), servingNodesCount);
      if (servingNodesCount < minimalShardReplicationCount) {
        minimalShardReplicationCount = servingNodesCount;
      }
      if (servingNodesCount > maximaShardReplicationCount) {
        maximaShardReplicationCount = servingNodesCount;
      }
    }
    return new ReplicationReport(replicationCountByShardMap, desiredReplicationCount, minimalShardReplicationCount,
            maximaShardReplicationCount);
  }

  public MasterQueue publishMaster(final Master master) {
    String masterName = master.getMasterName();
    String zkMasterPath = _zkConf.getZkPath(PathDef.MASTER);
    cleanupOldMasterData(masterName, zkMasterPath);

    boolean isMaster;
    if (!_zkClient.exists(zkMasterPath)) {
      LOG.info(masterName + " starting as master...");
      createEphemeral(master, zkMasterPath, new MasterMetaData(masterName, System.currentTimeMillis()));
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

    MasterQueue queue = null;
    if (isMaster) {
      LOG.info("master '" + master.getMasterName() + "' published");
      String queuePath = _zkConf.getZkPath(PathDef.MASTER_QUEUE);
      if (!_zkClient.exists(queuePath)) {
        _zkClient.createPersistent(queuePath);
      }
      queue = new MasterQueue(_zkClient, queuePath);
    } else {
      LOG.info("secondary master '" + master.getMasterName() + "' registered");
    }

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

  public NodeQueue publishNode(Node node, NodeMetaData nodeMetaData) {
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
    if (_zkClient.exists(queuePath)) {
      _zkClient.deleteRecursive(queuePath);
    }

    NodeQueue nodeQueue = new NodeQueue(_zkClient, queuePath);

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

  public void publishShard(Node node, String shardName) {
    // announce that this node serves this shard now...
    final String shard2NodePath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      _zkClient.delete(shard2NodePath);
    }
    _zkClient.createPersistent(_zkConf.getZkPath(PathDef.SHARD_TO_NODES, shardName), true);
    createEphemeral(node, shard2NodePath, null);
  }

  public void unpublishShard(Node node, String shard) {
    String shard2NodePath = _zkConf.getZkPath(PathDef.SHARD_TO_NODES, shard, node.getName());
    if (_zkClient.exists(shard2NodePath)) {
      _zkClient.delete(shard2NodePath);
    }
    _zkEphemeralPublishesByComponent.removeValue(node, shard2NodePath);
  }

  public void setMetric(String nodeName, MetricsRecord metricsRecord) {
    String metricsPath = _zkConf.getZkPath(PathDef.NODE_METRICS, nodeName);
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
    return (MetricsRecord) readZkData(_zkConf.getZkPath(PathDef.NODE_METRICS, nodeName));
  }

  private void createEphemeral(ConnectedComponent component, String path, Serializable content) {
    _zkClient.createEphemeral(path, content);
    _zkEphemeralPublishesByComponent.add(component, path);
  }

  public void addMasterOperation(MasterOperation operation) {
    String queuePath = _zkConf.getZkPath(PathDef.MASTER_QUEUE);
    new MasterQueue(_zkClient, queuePath).add(operation);
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

  private NodeQueue getNodeQueue(String nodeName) {
    String queuePath = _zkConf.getZkPath(PathDef.NODE_QUEUE, nodeName);
    return new NodeQueue(_zkClient, queuePath);
  }

  public boolean indexExists(String indexName) {
    return _zkClient.exists(_zkConf.getZkPath(PathDef.INDICES_METADATA, indexName));
  }

  public void showStructure(final boolean all) {
    final String string;
    if (all) {
      string = ZkPathUtil.toString(_zkClient, _zkConf.getZkRootPath(), PathFilter.ALL);
    } else {
      final Set<String> nonViPathes = new HashSet<String>();
      for (PathDef pathDef : PathDef.values()) {
        if (!pathDef.isVip()) {
          nonViPathes.add(_zkConf.getZkPath(pathDef));
        }
      }
      string = ZkPathUtil.toString(_zkClient, _zkConf.getZkRootPath(), new PathFilter() {
        @Override
        public boolean showChilds(String path) {
          return !nonViPathes.contains(path);
        }
      });
    }
    System.out.println(string);
  }

  public void explainStructure() {
    for (PathDef pathDef : PathDef.values()) {
      String zkPath = _zkConf.getZkPath(pathDef);
      System.out.println(StringUtil.fillWithWhiteSpace(zkPath, 40) + "\t" + pathDef.getDescription());
    }
  }

  static class AddRemoveListenerAdapter implements IZkChildListener {

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
    public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
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
