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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.node.ShardUndeployInstruction;
import net.sf.katta.util.CollectionUtil;

public class IndexUndeployOperation implements LeaderOperation {

  private static final long serialVersionUID = 1L;
  private final String _indexName;

  public IndexUndeployOperation(String indexName) {
    _indexName = indexName;
  }

  @Override
  public void execute(LeaderContext context) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    IndexMetaData indexMD = protocol.getIndexMD(_indexName);

    Map<String, List<String>> shard2NodesMap = new HashMap<String, List<String>>();
    Set<Shard> shards = indexMD.getShards();
    for (Shard shard : shards) {
      shard2NodesMap.put(shard.getName(), protocol.getShardNodes(shard.getName()));
    }

    Map<String, List<String>> node2ShardsMap = CollectionUtil.invertListMap(shard2NodesMap);
    Set<String> nodes = node2ShardsMap.keySet();
    for (String node : nodes) {
      List<String> nodeShards = node2ShardsMap.get(node);
      protocol.addNodeOperation(node, new ShardUndeployInstruction(nodeShards));
    }
  }

  @Override
  public void nodeOperationComplete(LeaderContext context) throws Exception {
    // nothing todo
  }

}
