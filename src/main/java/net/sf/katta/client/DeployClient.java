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
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;

public class DeployClient implements IDeployClient {

  private final InteractionProtocol _protocol;

  public DeployClient(ZkClient zkClient, ZkConfiguration configuration) {
    this(new InteractionProtocol(zkClient, configuration));
  }

  public DeployClient(InteractionProtocol interactionProtocol) {
    _protocol = interactionProtocol;
  }

  public IIndexDeployFuture addIndex(String indexName, String indexPath, int replicationLevel) {
    validateIndexName(indexName, indexPath);
    _protocol.addIndex(indexName, indexPath, replicationLevel);
    return new IndexDeployFuture(_protocol, indexName);
  }

  private void validateIndexName(String name, String indexPath) {
    if (name.trim().equals("*")) {
      throw new IllegalArgumentException("invalid index name: " + name);
    }
  }

  public void removeIndex(String indexName) {
    _protocol.removeIndex(indexName);
  }

  public boolean existsIndex(String indexName) {
    return _protocol.indexExists(indexName);
  }

  public List<IndexMetaData> getIndexes(IndexState indexState) {
    return _protocol.getIndicesMDs(indexState);
  }

  public List<String> getIndexNames(IndexState indexState) {
    List<IndexMetaData> indexesMds = getIndexes(indexState);
    final List<String> returnIndexes = new ArrayList<String>();
    for (final IndexMetaData indexMD : indexesMds) {
      returnIndexes.add(indexMD.getName());
    }
    return returnIndexes;
  }

  public IndexMetaData getIndexMetaData(String indexName) {
    return _protocol.getIndexMD(indexName);
  }
}
