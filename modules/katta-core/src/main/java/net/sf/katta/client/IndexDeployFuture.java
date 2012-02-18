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
package net.sf.katta.client;

import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

public class IndexDeployFuture implements IIndexDeployFuture, IZkDataListener, ConnectedComponent {

  private static Logger LOG = Logger.getLogger(IndexDeployFuture.class);

  private final InteractionProtocol _protocol;
  private final String _indexName;
  private volatile boolean _registered;

  public IndexDeployFuture(InteractionProtocol protocol, String indexName) {
    _protocol = protocol;
    _indexName = indexName;
    _protocol.registerComponent(this);
    _protocol.registerDataListener(this, PathDef.INDICES_METADATA, indexName, this);
    _registered = true;
  }

  public synchronized IndexState getState() {
    if (!_registered) {
      return IndexState.DEPLOYED;
    }

    IndexMetaData indexMD = _protocol.getIndexMD(_indexName);
    if (indexMD == null) {
      return IndexState.DEPLOYING;
    } else if (indexMD.hasDeployError()) {
      return IndexState.ERROR;
    }
    if (_registered) {
      _registered = false;
      _protocol.unregisterComponent(this);
    }
    return IndexState.DEPLOYED;
  }

  private boolean isDeploymentRunning() {
    return getState() == IndexState.DEPLOYING;
  }

  public synchronized IndexState joinDeployment() throws InterruptedException {
    while (isDeploymentRunning()) {
      wait(3000);
    }
    return getState();
  }

  public synchronized IndexState joinDeployment(long maxTime) throws InterruptedException {
    long startJoin = System.currentTimeMillis();
    while (isDeploymentRunning()) {
      wait(maxTime);
      maxTime = maxTime - (System.currentTimeMillis() - startJoin);
      if (maxTime <= 0) {
        break;
      }
    }
    return getState();
  }

  @Override
  public synchronized void handleDataChange(String dataPath, Object data) {
    wakeSleeper();
  }

  public synchronized void handleDataDeleted(String dataPath) {
    _registered = false;
    _protocol.unregisterComponent(this);
    wakeSleeper();
  }

  private synchronized void wakeSleeper() {
    notifyAll();
  }

  @Override
  public void reconnect() {
    // sg: we just want to make sure we get the very latest state of the
    // index, since we might missed a event. With zookeeper 3.x we should still
    // have subscribed notifications and don't need to re-subscribe
    LOG.warn("Reconnecting IndexDeployFuture");
    wakeSleeper();
  }

  @Override
  public void disconnect() {
    // nothing
  }
}
