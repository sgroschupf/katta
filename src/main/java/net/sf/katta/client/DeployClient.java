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

import java.util.List;

import net.sf.katta.operation.master.AbstractIndexOperation;
import net.sf.katta.operation.master.IndexDeployOperation;
import net.sf.katta.operation.master.IndexUndeployOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.util.ZkConfiguration;

import org.I0Itec.zkclient.ZkClient;

public class DeployClient implements IDeployClient {

  private final InteractionProtocol _protocol;

  /**
   *@deprecated use {@link #DeployClient(InteractionProtocol)} instead
   */
  public DeployClient(ZkClient zkClient, ZkConfiguration configuration) {
    this(new InteractionProtocol(zkClient, configuration));
  }

  public DeployClient(InteractionProtocol interactionProtocol) {
    _protocol = interactionProtocol;
  }

  @Override
  public IIndexDeployFuture addIndex(String indexName, String indexPath, int replicationLevel) {
    validateIndexData(indexName, replicationLevel);
    _protocol.addMasterOperation(new IndexDeployOperation(indexName, indexPath, replicationLevel));
    return new IndexDeployFuture(_protocol, indexName);
  }

  private void validateIndexData(String name, int replicationLevel) {
    // TODO jz: try to access path already ?
    if (replicationLevel <= 0) {
      throw new IllegalArgumentException("replication level must be 1 or greater");
    }
    if (name.trim().equals("*")) {
      throw new IllegalArgumentException("invalid index name: " + name);
    }
    if (name.contains(AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR + "")) {
      throw new IllegalArgumentException("invalid index name : " + name + " - must not contain "
              + AbstractIndexOperation.INDEX_SHARD_NAME_SEPARATOR);
    }
  }

  @Override
  public void removeIndex(String indexName) {
    if (!existsIndex(indexName)) {
      throw new IllegalArgumentException("index with name '" + indexName + "' does not exists");
    }
    _protocol.addMasterOperation(new IndexUndeployOperation(indexName));
  }

  @Override
  public boolean existsIndex(String indexName) {
    return _protocol.indexExists(indexName);
  }

  @Override
  public IndexMetaData getIndexMetaData(String indexName) {
    return _protocol.getIndexMD(indexName);
  }

  @Override
  public List<String> getIndices() {
    return _protocol.getIndices();
  }
}
