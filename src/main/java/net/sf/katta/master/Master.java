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

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.operation.leader.CheckIndicesOperation;
import net.sf.katta.protocol.operation.leader.LeaderOperation;
import net.sf.katta.protocol.operation.leader.RemoveSuperfluousShardsOperation;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;

public class Master implements ConnectedComponent {

  protected final static Logger LOG = Logger.getLogger(Master.class);

  protected volatile OperatorThread _operatorThread;

  private String _masterName;
  private ZkServer _zkServer;
  private boolean _shutdownClient;
  protected InteractionProtocol _protocol;

  private LeaderContext _context;
  private long _safeModeMaxTime;

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
    try {
      final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class.forName(deployPolicyClassName);
      IDeployPolicy _deployPolicy = policyClazz.newInstance();
      _context = new LeaderContext(_protocol, _deployPolicy);
    } catch (final Exception e) {
      throw new KattaException("Unable to instantiate deploy policy", e);
    }

    _safeModeMaxTime = 10000;
    if (!masterConfiguration.containsProperty(MasterConfiguration.SAFE_MODE_MAX_TIME)) {
      LOG.warn(MasterConfiguration.SAFE_MODE_MAX_TIME + " not configured in master configuration");
      // TODO jz: remove that check once we can assume all config files has been
      // updated
    } else {
      _safeModeMaxTime = masterConfiguration.getInt(MasterConfiguration.SAFE_MODE_MAX_TIME);
    }

  }

  public void start() {
    becomePrimaryOrSecondaryMaster();
  }

  @Override
  public void reconnect() {
    becomePrimaryOrSecondaryMaster();
  }

  @Override
  public void disconnect() {
    _operatorThread.interrupt();
    try {
      _operatorThread.join();
    } catch (InterruptedException e) {
      Thread.interrupted();
      // proceed
    }
    _operatorThread = null;
  }

  private void becomePrimaryOrSecondaryMaster() {
    OperationQueue<LeaderOperation> queue = _protocol.publishMaster(this);
    if (queue != null) {
      startNodeManagement();
      _operatorThread = new OperatorThread(_context, queue, _safeModeMaxTime);
      _operatorThread.start();
    }
  }

  public boolean isInSafeMode() {
    return _operatorThread.isInSafeMode();
  }

  public Collection<String> getConnectedNodes() {
    return _protocol.getLiveNodes();
  }

  public boolean isMaster() {
    return _operatorThread != null;
  }

  public String getMasterName() {
    return _masterName;
  }

  public void handleMasterDisappearedEvent() {
    becomePrimaryOrSecondaryMaster();
  }

  private void startNodeManagement() {
    LOG.info("start managing nodes...");
    List<String> nodes = _protocol.registerNodeListener(this, new IAddRemoveListener() {
      @Override
      public void removed(String name) {
        _protocol.addLeaderOperation(new CheckIndicesOperation());
      }

      @Override
      public void added(String name) {
        _protocol.addLeaderOperation(new RemoveSuperfluousShardsOperation(name));
        _protocol.addLeaderOperation(new CheckIndicesOperation());
      }
    });
    LOG.info("found following nodes connected: " + nodes);
  }

  public void shutdown() {
    if (_protocol != null) {
      _protocol.unregisterComponent(this);
      if (isMaster()) {
        _operatorThread.interrupt();
        try {
          _operatorThread.join();
        } catch (final InterruptedException e1) {
          // proceed
        }
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
  }

}
