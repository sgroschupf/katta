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
package net.sf.katta.operation.node;

import static org.mockito.Mockito.mock;
import net.sf.katta.AbstractTest;
import net.sf.katta.node.IContentServer;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeContext;
import net.sf.katta.node.ShardManager;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.testutil.Mocks;

public class AbstractNodeOperationMockTest extends AbstractTest {

  protected InteractionProtocol _protocol = mock(InteractionProtocol.class);
  protected Node _node = Mocks.mockNode();
  protected ShardManager _shardManager = mock(ShardManager.class);
  protected IContentServer _contentServer = mock(IContentServer.class);
  protected NodeContext _context = new NodeContext(_protocol, _node, _shardManager, _contentServer);

}
