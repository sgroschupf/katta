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
    _zkClient.subscribeDataChanges(_indexZkPath, this);
    _zkClient.readData(_indexZkPath, indexMetaData);
    _zkClient.getEventLock().unlock();
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

  public synchronized IndexState joinReplication() throws InterruptedException {
    while (_indexMetaData.getState() != IndexState.REPLICATING || _indexMetaData.getState() != IndexState.DEPLOYED
        || _indexMetaData.getState() != IndexState.ERROR) {
      this.wait();
    }
    return getState();
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
