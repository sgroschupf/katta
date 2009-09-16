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

import java.io.Serializable;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;

//TODO PVo test all edge cases
public class IndexDeployFuture implements IIndexDeployFuture, IZkDataListener, IZkStateListener {

  private static Logger LOG = Logger.getLogger(IndexDeployFuture.class);

  private final ZkClient _zkClient;
  private final String _indexZkPath;
  private IndexMetaData _indexMetaData;

  public IndexDeployFuture(ZkClient zkClient, String index, String indexZkPath, IndexMetaData indexMetaData) {
    _zkClient = zkClient;
    _indexMetaData = indexMetaData;
    _indexZkPath = indexZkPath;

    _zkClient.subscribeStateChanges(this);
    _zkClient.subscribeDataChanges(_indexZkPath, this);
    _indexMetaData = _zkClient.readData(_indexZkPath);
  }

  public synchronized IndexState getState() {
    return _indexMetaData.getState();
  }

  public synchronized IndexState joinDeployment() throws InterruptedException {
    while (isDeploymentRunning(_indexMetaData)) {
      wait(5000);
    }
    return getState();
  }

  public synchronized IndexState joinDeployment(long maxTime) throws InterruptedException {
    long startJoin = System.currentTimeMillis();
    while (isDeploymentRunning(_indexMetaData)) {
      wait(maxTime);
      maxTime = maxTime - (System.currentTimeMillis() - startJoin);
      if (maxTime <= 0) {
        break;
      }
    }
    return getState();
  }

  private boolean isDeploymentRunning(IndexMetaData indexMetaData) {
    if (indexMetaData == null) {
      return false;
    }
    return indexMetaData.getState() != IndexState.DEPLOYED && indexMetaData.getState() != IndexState.ERROR;
  }

  @Override
  public synchronized void handleDataChange(String dataPath, Serializable data) {
    updateIndexMetaData((IndexMetaData) data);
  }

  public synchronized void handleDataDeleted(String dataPath) {
    // index got deleted
    updateIndexMetaData(null);
    notifyAll();
  }

  private synchronized void updateIndexMetaData(IndexMetaData data) {
    _indexMetaData = data;
    notifyAll();

    if (!isDeploymentRunning(data)) {
      _zkClient.unsubscribeDataChanges(_indexZkPath, this);
    }
  }

  @Override
  public void handleNewSession() throws Exception {
    // ignore
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    if (state == KeeperState.SyncConnected) {
      // sg: we just want to make sure we get the very latest state of the
      // index,
      // since we might missed a event. With zookeeper 3.x we should still have
      // subcribed notifcatins and dont need to resubscribe
      LOG.warn("Reconnecting IndexDeployFuture");
      updateIndexMetaData((IndexMetaData) _zkClient.readData(_indexZkPath));
    }
  }
}
