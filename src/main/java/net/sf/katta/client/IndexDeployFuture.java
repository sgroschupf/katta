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

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.KattaException;
import net.sf.katta.zk.IZkDataListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

public class IndexDeployFuture implements IIndexDeployFuture, IZkDataListener<IndexMetaData> {

  private final ZKClient _zkClient;
  private final String _indexZkPath;
  private IndexMetaData _indexMetaData;

  public IndexDeployFuture(ZKClient zkClient, String index, IndexMetaData indexMetaData) throws KattaException {
    _zkClient = zkClient;
    _indexMetaData = indexMetaData;
    _indexZkPath = ZkPathes.getIndexPath(index);

    // subscribe index
    _zkClient.getEventLock().lock();
    try {
      _zkClient.subscribeDataChanges(_indexZkPath, this);
      _zkClient.readData(_indexZkPath, _indexMetaData);
    } finally {
      _zkClient.getEventLock().unlock();
    }
  }

  public synchronized IndexState getState() {
    return _indexMetaData.getState();
  }

  public synchronized IndexState joinDeployment() throws InterruptedException {
    while (isDeploymentRunning()) {
      this.wait();
    }
    return getState();
  }

  public synchronized IndexState joinDeployment(long maxTime) throws InterruptedException {
    long startJoin = System.currentTimeMillis();
    while (isDeploymentRunning()) {
      this.wait(maxTime);
      maxTime = maxTime - (System.currentTimeMillis() - startJoin);
      if (maxTime <= 0) {
        break;
      }
    }
    return getState();
  }

  private boolean isDeploymentRunning() {
    return _indexMetaData.getState() != IndexState.DEPLOYED && _indexMetaData.getState() != IndexState.ERROR;
  }

  public synchronized void handleDataAdded(String dataPath, IndexMetaData data) throws KattaException {
    updateIndexMetaData(data);
  }

  public synchronized void handleDataChange(String dataPath, IndexMetaData data) throws KattaException {
    updateIndexMetaData(data);
  }

  public synchronized void handleDataDeleted(String dataPath) throws KattaException {
    // index got deleted
    this.notifyAll();
  }

  private void updateIndexMetaData(IndexMetaData data) {
    _indexMetaData = data;
    this.notifyAll();

    if (data.getState() == IndexState.DEPLOYED || data.getState() == IndexState.ERROR) {
      _zkClient.unsubscribeDataChanges(_indexZkPath, this);
    }
  }

  public IndexMetaData createWritable() {
    return new IndexMetaData();
  }

}
