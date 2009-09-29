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
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;

public class DeployClient implements IDeployClient {

  private ZkConfiguration _conf;
  private ZkClient _zkClient;

  public DeployClient(ZkClient zkClient, ZkConfiguration configuration) {
    _conf = configuration;
    _zkClient = zkClient;
  }

  public IIndexDeployFuture addIndex(String name, String path, int replicationLevel) {
    final String indexPath = _conf.getZKIndexPath(name);
    validateIndexName(name, indexPath);
    final IndexMetaData indexMetaData = new IndexMetaData(name, path, replicationLevel, IndexMetaData.IndexState.ANNOUNCED);
    _zkClient.createPersistent(indexPath, indexMetaData);
    return new IndexDeployFuture(_zkClient, name, indexPath, indexMetaData);
  }

  private void validateIndexName(String name, String indexPath) {
    if (name.trim().equals("*")) {
      throw new IllegalArgumentException("invalid index name: " + name);
    }

    if (_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index already exists: " + name);
    }
  }

  public void removeIndex(String name) {
    final String indexPath = _conf.getZKIndexPath(name);
    if (!_zkClient.exists(indexPath)) {
      throw new IllegalArgumentException("index not exists: " + name);
    }
    _zkClient.deleteRecursive(indexPath);
  }

  public boolean existsIndex(String indexName) {
    return _zkClient.exists(_conf.getZKIndexPath(indexName));
  }

  public List<IndexMetaData> getIndexes(IndexState indexState) {
    final List<String> indexes = _zkClient.getChildren(_conf.getZKIndicesPath());
    final List<IndexMetaData> returnIndexes = new ArrayList<IndexMetaData>();
    for (final String index : indexes) {
      final IndexMetaData metaData = _zkClient.readData(_conf.getZKIndexPath(index));
      if (metaData.getState() == indexState) {
        returnIndexes.add(metaData);
      }
    }
    return returnIndexes;
  }

  // TODO jz: if IndexMetaData would contain index name, we could avoid that
  public List<String> getIndexNames(IndexState indexState) {
    final List<String> indexes = _zkClient.getChildren(_conf.getZKIndicesPath());
    final List<String> returnIndexes = new ArrayList<String>();
    for (final String index : indexes) {
      final IndexMetaData metaData = _zkClient.readData(_conf.getZKIndexPath(index));
      if (metaData.getState() == indexState) {
        returnIndexes.add(index);
      }
    }
    return returnIndexes;
  }

  public IndexMetaData getIndexMetaData(String name) {
    return _zkClient.readData(_conf.getZKIndexPath(name));
  }
}
