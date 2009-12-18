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
package net.sf.katta.protocol.operation.node;

import java.io.File;

import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.NodeContext;

public class ShardDeployOperation extends AbstractShardOperation {

  private static final long serialVersionUID = 1L;

  @Override
  protected String getOperationName() {
    return "deploy";
  }

  @Override
  protected void execute(NodeContext context, String shardName) throws Exception {
    String shardPath = getShardPath(shardName);
    File localShardFolder = context.getShardManager().installShard(shardName, shardPath);
    INodeManaged nodeManaged = context.getNodeManaged();
    if (!nodeManaged.getShards().contains(shardName)) {
      nodeManaged.addShard(shardName, localShardFolder);
    }
    announceShard(shardName, context);
  }

  @Override
  protected void onException(NodeContext context, String shardName, Exception e) {
    context.getShardManager().uninstallShard(shardName);
  }

}
