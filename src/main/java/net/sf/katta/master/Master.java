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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

public class Master implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Master.class);

  protected DistributeShardsThread _manageShardThread;

  private String _masterName;
  protected boolean _isMaster;

  private Lock _shutdownLock = new ReentrantLock();

  private ZkServer _zkServer;
  private boolean _shutdownClient;
  private InteractionProtocol _protocol;

  public Master(InteractionProtocol interactionProtocol, ZkServer zkServer) throws KattaException {
    this(interactionProtocol, false);
    _zkServer = zkServer;
  }

  public Master(InteractionProtocol interactionProtocol, boolean shutdownClient) throws KattaException {
    this(interactionProtocol, shutdownClient, new MasterConfiguration());
  }

  @SuppressWarnings("unchecked")
  public Master(InteractionProtocol interactionProtocol, boolean shutdownClient, MasterConfiguration masterConfiguration)
          throws KattaException {
    _protocol = interactionProtocol;
    _masterName = NetworkUtil.getLocalhostName() + "_" + UUID.randomUUID().toString();
    _shutdownClient = shutdownClient;
    interactionProtocol.registerComponent(this);
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
    _manageShardThread = new DistributeShardsThread(_protocol, deployPolicy, safeModeMaxTime);
  }

  public void start() {
    becomePrimaryOrSecondaryMaster();
  }

  private void becomePrimaryOrSecondaryMaster() {
    _isMaster = _protocol.becomeMasterOrSecondaryMaster(this);
    if (_isMaster) {
      startNodeManagement();
      startIndexManagement();
      _manageShardThread.start();
    }
  }

  public boolean isInSafeMode() {
    return _manageShardThread.isInSafeMode();
  }

  public void shutdown() {
    _shutdownLock.lock();
    try {
      if (_protocol != null) {
        _protocol.unregisterComponent(this);
        _manageShardThread.interrupt();
        try {
          _manageShardThread.join();
        } catch (final InterruptedException e1) {
          // proceed
        }
        if (_shutdownClient) {
          _protocol.disconnect();
        }
        _protocol = null;
      }
      if (_zkServer != null) {
        _zkServer.shutdown();
        _zkServer = null;
      }
    } finally {
      _shutdownLock.unlock();
    }
  }

  private void startIndexManagement() {
    LOG.debug("Loading indexes...");
    List<String> indices = _protocol.registerIndexListener(this, new IAddRemoveListener() {
      @Override
      public void removed(String name) {
        _manageShardThread.removeIndex(name);
      }

      @Override
      public void added(String name) {
        _manageShardThread.addIndex(name);
      }
    });
    LOG.info("found following indices connected: " + indices);
  }

  private void startNodeManagement() {
    LOG.info("start managing nodes...");
    List<String> nodes = _protocol.registerNodeListener(this, new IAddRemoveListener() {
      @Override
      public void removed(String name) {
        _manageShardThread.removeNode(name);
      }

      @Override
      public void added(String name) {
        _manageShardThread.addNode(name);
      }
    });
    LOG.info("found following nodes connected: " + nodes);
  }

  protected List<String> readNodes() {
    return _protocol.getNodes();
  }

  protected List<String> readIndexes() {
    return _protocol.getIndices();
  }

  public boolean isMaster() {
    return _isMaster;
  }

  @Deprecated
  public List<String> getNodes() {
    return Collections.unmodifiableList(readNodes());
  }

  @Deprecated
  public List<String> getIndexes() {
    return Collections.unmodifiableList(readIndexes());
  }

  public String getMasterName() {
    return _masterName;
  }

  public void handleMasterDisappearedEvent() {
    becomePrimaryOrSecondaryMaster();
  }

  @Override
  public void reconnect() {
    becomePrimaryOrSecondaryMaster();
  }

  @Override
  public void disconnect() {
    // TODO ??
  }

}
