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
package net.sf.katta.protocol.operation.leader;

import java.util.Set;

import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;

import org.apache.hadoop.util.StringUtils;

public class BalanceIndexOperation extends AbstractDistributeShardsOperation {

  private static final long serialVersionUID = 1L;

  private final String _indexName;

  public BalanceIndexOperation(String indexName) {
    _indexName = indexName;
  }

  @Override
  public void execute(LeaderContext context) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    IndexMetaData indexMetaData = protocol.getIndexMD(_indexName);
    LOG.info("balancing shards for index '" + _indexName + "'");

    // TODO should write AssignedShard to indexPath/shard (so we can read from
    // there)
    // final Map<String, AssignedShard> shard2AssignedShardMap =
    // readShardsFromFs(_indexName, indexMetaData);
    final Set<Shard> indexShards = indexMetaData.getShards();
    LOG.info("Found shards '" + indexShards + "' for index '" + _indexName + "'");

    try {
      indexMetaData.setState(IndexState.REPLICATING);
      protocol.updateIndexMD(indexMetaData);
      distributeIndexShards(protocol, context.getDeployPolicy(), indexMetaData, protocol.getNodes(), false);
    } catch (Exception e) {
      indexMetaData.setState(IndexState.ERROR, StringUtils.stringifyException(e));
      protocol.updateIndexMD(indexMetaData);
    }
  }

  @Override
  public void nodeOperationComplete(LeaderContext context) throws Exception {
    // TODO check if index is well balanced and re-execute if not ?
    // scan queue and reexecute only if no balance op for this index is
    // contained ?
  }
}
