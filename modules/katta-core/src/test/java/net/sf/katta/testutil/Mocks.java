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
package net.sf.katta.testutil;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.MasterQueue;
import net.sf.katta.protocol.NodeQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;

public class Mocks {

  private static int _nodeCounter;
  private static int _masterCounter;

  public static Master mockMaster() {
    Master master = mock(Master.class);
    when(master.getMasterName()).thenReturn("master" + _masterCounter++);
    return master;
  }

  public static MasterQueue publishMaster(InteractionProtocol protocol) {
    Master master = mockMaster();
    return protocol.publishMaster(master);
  }

  public static Node mockNode() {
    Node node = mock(Node.class);
    when(node.getName()).thenReturn("node" + _nodeCounter++);
    return node;
  }

  public static List<Node> mockNodes(int count) {
    List<Node> nodes = new ArrayList<Node>();
    for (int i = 0; i < count; i++) {
      nodes.add(mockNode());
    }
    return nodes;
  }

  public static NodeQueue publisNode(InteractionProtocol protocol, Node node) {
    return protocol.publishNode(node, new NodeMetaData(node.getName()));
  }

  public static List<NodeQueue> publisNodes(InteractionProtocol protocol, List<Node> nodes) {
    List<NodeQueue> nodeQueues = new ArrayList<NodeQueue>();
    for (Node node : nodes) {
      nodeQueues.add(publisNode(protocol, node));
    }
    return nodeQueues;
  }

}
