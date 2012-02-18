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
package net.sf.katta.node;

import net.sf.katta.protocol.InteractionProtocol;

public class NodeContext {

  private final Node _node;
  private final ShardManager _shardManager;
  private final InteractionProtocol _protocol;
  private final IContentServer _nodeManaged;

  public NodeContext(InteractionProtocol protocol, Node node, ShardManager shardManager, IContentServer nodeManaged) {
    _protocol = protocol;
    _node = node;
    _shardManager = shardManager;
    _nodeManaged = nodeManaged;
  }

  public Node getNode() {
    return _node;
  }

  public ShardManager getShardManager() {
    return _shardManager;
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  public IContentServer getContentServer() {
    return _nodeManaged;
  }
}
