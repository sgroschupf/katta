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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Master implements IZkReconnectListener {

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

  @SuppressWarnings("unchecked")
  public Master(final ZkClient zkClient) {
    _masterName = NetworkUtil.getLocalhostName() + "_" + UUID.randomUUID().toString();
    _indexListener = new IndexListener();
    _nodeListener = new NodeListener();
    _masterLister = new MasterListener();
    _conf = zkClient.getConfig();
    if (!_conf.getZKRootPath().equals(ZkConfiguration.DEFAULT_ROOT_PATH)) {
      LOG.info("Using ZK root path: " + _conf.getZKRootPath());
    }
    _zkClient = zkClient;
    try {
      _zkClient.getEventLock().lock();
      _zkClient.subscribeReconnects(this);
    } finally {
      _zkClient.getEventLock().unlock();
    }
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
    _manageShardThread = new DistributeShardsThread(_zkClient, deployPolicy, safeModeMaxTime, false);
  }

  public void start() {
    try {
      _zkClient.getEventLock().lock();
      if (!_zkClient.isStarted()) {
        LOG.info("connecting with zookeeper");
        _zkClient.start(300000);
        // now we need to create the default name space
        _zkClient.createDefaultNameSpace();
      }
      becomeMasterOrSecondaryMaster();
      if (_isMaster) {
        _zkClient.createDefaultNameSpace();
        startNodeManagement();
        startIndexManagement();
        _manageShardThread.start();
      }
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  public boolean isInSafeMode() {
    return _manageShardThread.isInSafeMode();
  }

  public void shutdown() {
    try {
      _manageShardThread.interrupt();
      try {
        _manageShardThread.join();
      } catch (final InterruptedException e1) {
        // proceed
      }
      _zkClient.getEventLock().lock();
      try {
        _zkClient.unsubscribeAll();
        _zkClient.delete(_conf.getZKMasterPath());
      } catch (final KattaException e) {
        LOG.error("Could not delete the master data from zk");
      }
      _zkClient.close();
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  private void becomeMasterOrSecondaryMaster() throws KattaException {
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

  private void cleanupOldMasterData(final String masterName) throws KattaException {
    if (_zkClient.exists(_conf.getZKMasterPath())) {
      final MasterMetaData existingMaster = new MasterMetaData("", System.currentTimeMillis());
      _zkClient.readData(_conf.getZKMasterPath(), existingMaster);
      if (existingMaster.getMasterName().equals(masterName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(_conf.getZKMasterPath());
      }
    }
  }

  private void startIndexManagement() throws KattaException {
    LOG.debug("Loading indexes...");
    _indexes = _zkClient.subscribeChildChanges(_conf.getZKIndicesPath(), _indexListener);
    _manageShardThread.updateIndexes(_indexes);
  }

  private void startNodeManagement() throws KattaException {
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

  protected class MasterListener implements IZkDataListener<MasterMetaData> {

    public void handleDataAdded(final String dataPath, final MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataChange(final String dataPath, final MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataDeleted(final String dataPath) throws KattaException {
      if (!_isMaster) {
        // start from scratch again...
        LOG.info("An master failure was detected...");
        try {
          start();
        } catch (final KattaException e) {
          LOG.error("Faild to process Master change notificaiton.", e);
        }
      }
    }

    public MasterMetaData createWritable() {
      return new MasterMetaData();
    }

  }

  protected List<String> readNodes() throws KattaException {
    return _zkClient.getChildren(_conf.getZKNodesPath());
  }

  protected List<String> readIndexes() throws KattaException {
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
