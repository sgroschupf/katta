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

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

public class DeployClient implements IDeployClient {

  private ZKClient _zkClient;

  public DeployClient(ZkConfiguration zkConfiguration) throws KattaException {
    this(new ZKClient(zkConfiguration));
  }

  public DeployClient(ZKClient zkClient) throws KattaException {
    _zkClient = zkClient;
    if (!_zkClient.isStarted()) {
      _zkClient.start(30000);
    }
  }

  public IIndexDeployFuture addIndex(String name, String path, String analyzerClass, int replicationLevel)
      throws KattaException {
    final String indexPath = ZkPathes.getIndexPath(name);
    validateIndexName(name, indexPath);

    final IndexMetaData indexMetaData = new IndexMetaData(path, analyzerClass, replicationLevel,
        IndexMetaData.IndexState.ANNOUNCED);
    _zkClient.create(indexPath, indexMetaData);
    return new IndexDeployFuture(_zkClient, name, indexMetaData);
  }

  private void validateIndexName(String name, String indexPath) throws KattaException {
    if (name.trim().equals("*")) {
      throw new IllegalArgumentException("invalid index name: " + name);
    }

    if (_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index already exists: " + name);
    }
  }

  public void removeIndex(String name) throws KattaException {
    final String indexPath = ZkPathes.getIndexPath(name);
    if (!_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index not exists: " + name);
    }
    _zkClient.deleteRecursive(indexPath);
  }

  public boolean existsIndex(String indexName) throws KattaException {
    return _zkClient.exists(ZkPathes.getIndexPath(indexName));
  }

  public List<IndexMetaData> getIndexes(IndexState indexState) throws KattaException {
    final List<String> indexes = _zkClient.getChildren(ZkPathes.INDEXES);
    final List<IndexMetaData> returnIndexes = new ArrayList<IndexMetaData>();
    for (final String index : indexes) {
      final IndexMetaData metaData = new IndexMetaData();
      _zkClient.readData(ZkPathes.getIndexPath(index), metaData);
      if (metaData.getState() == indexState) {
        returnIndexes.add(metaData);
      }
    }
    return returnIndexes;
  }

  // TODO jz: if IndexMetaData would contain index name, we could avoid that
  public List<String> getIndexNames(IndexState indexState) throws KattaException {
    final List<String> indexes = _zkClient.getChildren(ZkPathes.INDEXES);
    final List<String> returnIndexes = new ArrayList<String>();
    for (final String index : indexes) {
      final IndexMetaData metaData = new IndexMetaData();
      _zkClient.readData(ZkPathes.getIndexPath(index), metaData);
      if (metaData.getState() == indexState) {
        returnIndexes.add(index);
      }
    }
    return returnIndexes;
  }

  public void disconnect() {
    _zkClient.close();
  }

}
