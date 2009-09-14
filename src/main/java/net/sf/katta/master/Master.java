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
package net.sf.katta.master;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.DefaultNameSpaceImpl;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Master implements IZkStateListener {

  protected final static Logger LOG = Logger.getLogger(Master.class);

  protected DistributeShardsThread _manageShardThread;
  protected ZkConfiguration _conf;
  protected ZkClient _zkClient;

  protected List<String> _nodes = new ArrayList<String>();
  protected List<String> _indexes = new ArrayList<String>();

  protected boolean _isMaster;

  private String _masterName;

  private IndexListener _indexListener;

  private NodeListener _nodeListener;

  private MasterListener _masterLister;
  private Lock _shutdownLock = new ReentrantLock();

  private ZkServer _zkServer;

  private boolean _shutdownClient;

  @SuppressWarnings("unchecked")
  public Master(ZkConfiguration conf, ZkClient zkClient, boolean shutdownClient) throws KattaException {
    _masterName = NetworkUtil.getLocalhostName() + "_" + UUID.randomUUID().toString();
    _indexListener = new IndexListener();
    _nodeListener = new NodeListener();
    _masterLister = new MasterListener();
    _shutdownClient = shutdownClient;
    _conf = conf;
    if (!_conf.getZKRootPath().equals(ZkConfiguration.DEFAULT_ROOT_PATH)) {
      LOG.info("Using ZK root path: " + _conf.getZKRootPath());
    }
    _zkClient = zkClient;
    _zkClient.subscribeStateChanges(this);
    final MasterConfiguration masterConfiguration = new MasterConfiguration();
    final String deployPolicyClassName = masterConfiguration.getDeployPolicy();
    IDeployPolicy deployPolicy;
    try {
      final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicyClassName);
      deployPolicy = policyClazz.newInstance();
    } catch (final Exception e) {
      throw new KattaException("Unable to instantiate deploy policy", e);
    }

    long safeModeMaxTime = 10000;

    if (!masterConfiguration.containsProperty(MasterConfiguration.SAFE_MODE_MAX_TIME)) {
      LOG.warn(MasterConfiguration.SAFE_MODE_MAX_TIME + " not configured in master configuration");
      // TODO jz: remove that check once we can assume all config files has been
      // updated
    } else {
      safeModeMaxTime = masterConfiguration.getInt(MasterConfiguration.SAFE_MODE_MAX_TIME);
    }
    _manageShardThread = new DistributeShardsThread(_conf, _zkClient, deployPolicy, safeModeMaxTime, false);
  }

  public Master(ZkConfiguration conf, ZkServer zkServer) throws KattaException {
    this(conf, zkServer.getZkClient(), false);
    _zkServer = zkServer;
  }

  public void start() {
    try {
      _zkClient.getEventLock().lock();
      // TODO PVo review this code
      // if (!_zkClient.isStarted()) {
      // LOG.info("connecting with zookeeper");
      // _zkClient.start(300000);
      // // it's now safe to create the namespace
      // createDefaultNamespace();
      // }
      becomeMasterOrSecondaryMaster();
      if (_isMaster) {
        createDefaultNamespace();
        startNodeManagement();
        startIndexManagement();
        _manageShardThread.start();
      }
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  private void createDefaultNamespace() {
    new DefaultNameSpaceImpl(_conf).createDefaultNameSpace(_zkClient);
  }

  public boolean isInSafeMode() {
    return _manageShardThread.isInSafeMode();
  }

  public void shutdown() {
    _shutdownLock.lock();
    try {
      if (_zkClient != null) {
        _manageShardThread.interrupt();
        try {
          _manageShardThread.join();
        } catch (final InterruptedException e1) {
          // proceed
        }
        _zkClient.unsubscribeAll();
        _zkClient.delete(_conf.getZKMasterPath());
        if (_shutdownClient) {
          _zkClient.close();
        }
        _zkClient = null;
      }
      if (_zkServer != null) {
        _zkServer.shutdown();
        _zkServer = null;
      }
    } finally {
      _shutdownLock.unlock();
    }
  }

  private void becomeMasterOrSecondaryMaster() {
    cleanupOldMasterData(_masterName);

    final MasterMetaData freshMaster = new MasterMetaData(_masterName, System.currentTimeMillis());
    if (!_zkClient.exists(_conf.getZKMasterPath())) {
      LOG.info(_masterName + " starting as master...");
      _isMaster = true;
      _zkClient.createEphemeral(_conf.getZKMasterPath(), freshMaster);
    } else {
      LOG.info(_masterName + " starting as secondary master...");
      _isMaster = false;
      _zkClient.subscribeDataChanges(_conf.getZKMasterPath(), _masterLister);
    }
  }

  private void cleanupOldMasterData(final String masterName) {
    if (_zkClient.exists(_conf.getZKMasterPath())) {
      final MasterMetaData existingMaster = _zkClient.readData(_conf.getZKMasterPath());
      if (existingMaster.getMasterName().equals(masterName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(_conf.getZKMasterPath());
      }
    }
  }

  private void startIndexManagement() {
    LOG.debug("Loading indexes...");
    _indexes = _zkClient.subscribeChildChanges(_conf.getZKIndicesPath(), _indexListener);
    _manageShardThread.updateIndexes(_indexes);
  }

  private void startNodeManagement() {
    LOG.info("start managing nodes...");
    _nodes = _zkClient.subscribeChildChanges(_conf.getZKNodesPath(), _nodeListener);
    if (!_nodes.isEmpty()) {
      LOG.info("found following nodes connected: " + _nodes);
    }
    _manageShardThread.updateNodes(_nodes);
  }

  protected class NodeListener implements IZkChildListener {

    public void handleChildChange(final String parentPath, final List<String> currentNodes) throws KattaException {
      LOG.info("got node event: " + currentNodes);
      _manageShardThread.updateNodes(currentNodes);
      _nodes = currentNodes;
    }
  }

  protected class IndexListener implements IZkChildListener {

    public void handleChildChange(final String parentPath, final List<String> currentIndexes) throws KattaException {
      LOG.info("got index event: " + currentIndexes);
      _manageShardThread.updateIndexes(currentIndexes);
      _indexes = currentIndexes;
    }
  }

  protected class MasterListener implements IZkDataListener {

    public void handleDataDeleted(final String dataPath) throws KattaException {
      if (!_isMaster) {
        // start from scratch again...
        LOG.info("An master failure was detected...");
        start();
      }
    }

    @Override
    public void handleDataChange(String dataPath, Serializable data) throws Exception {
      // do nothing
    }
  }

  protected List<String> readNodes() {
    return _zkClient.getChildren(_conf.getZKNodesPath());
  }

  protected List<String> readIndexes() {
    return _zkClient.getChildren(_conf.getZKIndicesPath());
  }

  public boolean isMaster() {
    return _isMaster;
  }

  public List<String> getNodes() {
    return Collections.unmodifiableList(_nodes);
  }

  public List<String> getIndexes() {
    return Collections.unmodifiableList(_indexes);
  }

  public String getMasterName() {
    return _masterName;
  }

  @Override
  public void handleNewSession() throws Exception {
    try {
      _zkClient.getEventLock().lock();
      becomeMasterOrSecondaryMaster();
      if (_isMaster) {
        startNodeManagement();
        startIndexManagement();
      }
    } finally {
      _zkClient.getEventLock().unlock();
    }

  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    // do nothing
  }
}
