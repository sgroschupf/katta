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

import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkDataListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.log4j.Logger;

public class Master {

  protected final static Logger LOG = Logger.getLogger(Master.class);

  protected DistributeShardsThread _manageShardThread;
  protected ZKClient _zkClient;

  protected List<String> _nodes = new ArrayList<String>();
  protected List<String> _indexes = new ArrayList<String>();

  protected boolean _isMaster;

  private int _safeModeMaxTime;

  public Master(final ZKClient zkClient) throws KattaException {
    _zkClient = zkClient;
    final MasterConfiguration masterConfiguration = new MasterConfiguration();
    final String deployPolicyClassName = masterConfiguration.getDeployPolicy();
    IDeployPolicy deployPolicy;
    try {
      final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicyClassName);
      deployPolicy = policyClazz.newInstance();
    } catch (final Exception e) {
      throw new KattaException("Unable to instantiate deploy policy", e);
    }

    if (!masterConfiguration.containsProperty(MasterConfiguration.SAFE_MODE_MAX_TIME)) {
      LOG.warn(MasterConfiguration.SAFE_MODE_MAX_TIME + " not configured in master configuration");
      _safeModeMaxTime = 10000;
      // TODO jz: remove that check once we can assume all config files has been
      // updated
    } else {
      _safeModeMaxTime = masterConfiguration.getInt(MasterConfiguration.SAFE_MODE_MAX_TIME);
    }
    _manageShardThread = new DistributeShardsThread(_zkClient, deployPolicy);
  }

  public void start() throws KattaException {
    try {
      _zkClient.getEventLock().lock();
      if (!_zkClient.isStarted()) {
        LOG.info("connecting with zookeeper");
        _zkClient.start(300000);
      }
      becomeMasterOrSecondaryMaster();
      if (_isMaster) {
        startNodeManagement();
        startIndexManagement();
        processSafeMode();
        _manageShardThread.start();
      }
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  public void processSafeMode() throws KattaException {
    LOG.info("entering safe mode (maximum " + _safeModeMaxTime + " ms)");
    try {
      // wait maximum safe mode time for new nodes
      Thread.sleep(_safeModeMaxTime);
    } catch (InterruptedException e) {
      throw new KattaException("interrupted safe mode", e);
    }
    LOG.info("leaving safe mode with " + _nodes.size() + " connected nodes");
  }

  public void shutdown() {
    try {
      _manageShardThread.interrupt();
      try {
        _manageShardThread.join();
      } catch (InterruptedException e1) {
        // proceed
      }
      _zkClient.getEventLock().lock();
      try {
        _zkClient.unsubscribeAll();
        _zkClient.delete(ZkPathes.MASTER);
      } catch (KattaException e) {
        LOG.error("could bot delete the master data from zk");
      }
      _zkClient.close();
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  private void becomeMasterOrSecondaryMaster() throws KattaException {
    final String hostName = NetworkUtil.getLocalhostName();
    cleanupOldMasterData(hostName);

    final MasterMetaData freshMaster = new MasterMetaData(hostName, System.currentTimeMillis());
    if (!_zkClient.exists(ZkPathes.MASTER)) {
      LOG.info(hostName + " starting as master...");
      _isMaster = true;
      _zkClient.createEphemeral(ZkPathes.MASTER, freshMaster);
    } else {
      LOG.info(hostName + " starting as secondary master...");
      _isMaster = false;
      _zkClient.subscribeDataChanges(ZkPathes.MASTER, new MasterListener());
    }
  }

  private void cleanupOldMasterData(final String hostName) throws KattaException {
    if (_zkClient.exists(ZkPathes.MASTER)) {
      final MasterMetaData existingMaster = new MasterMetaData(hostName, System.currentTimeMillis());
      _zkClient.readData(ZkPathes.MASTER, existingMaster);
      if (existingMaster.getMasterName().equals(hostName)) {
        LOG.warn("detected old master entry pointing to this host - deleting it..");
        _zkClient.delete(ZkPathes.MASTER);
      }
    }
  }

  private void startIndexManagement() throws KattaException {
    LOG.debug("Loading indexes...");
    _indexes = _zkClient.subscribeChildChanges(ZkPathes.INDEXES, new IndexListener());
    _manageShardThread.updateIndexes(_indexes);
    _manageShardThread.reportStartup();
  }

  private void startNodeManagement() throws KattaException {
    LOG.info("start managing nodes...");
    _nodes = _zkClient.subscribeChildChanges(ZkPathes.NODES, new NodeListener());
    if (!_nodes.isEmpty()) {
      LOG.info("found following nodes connected: " + _nodes);
    }
    _manageShardThread.updateNodes(_nodes);
  }

  protected class NodeListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentNodes) throws KattaException {
      LOG.info("got node event: " + currentNodes);
      _manageShardThread.updateNodes(currentNodes);
      _nodes = currentNodes;
    }
  }

  protected class IndexListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> currentIndexes) throws KattaException {
      LOG.info("got index event: " + currentIndexes);
      _manageShardThread.updateIndexes(currentIndexes);
      _indexes = currentIndexes;
    }
  }

  protected class MasterListener implements IZkDataListener<MasterMetaData> {

    public void handleDataAdded(String dataPath, MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataChange(String dataPath, MasterMetaData data) throws KattaException {
      // nothing todo
    }

    public void handleDataDeleted(String dataPath) throws KattaException {
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
    return _zkClient.getChildren(ZkPathes.NODES);
  }

  protected List<String> readIndexes() throws KattaException {
    return _zkClient.getChildren(ZkPathes.INDEXES);
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

}
