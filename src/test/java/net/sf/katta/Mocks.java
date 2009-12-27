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
package net.sf.katta;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.OperationQueue;
import net.sf.katta.protocol.metadata.NodeMetaData;

public class Mocks {

  private static int _nodeCounter;
  private static int _masterCounter;

  public static Master mockMaster() {
    Master master = mock(Master.class);
    when(master.getMasterName()).thenReturn("master" + _masterCounter++);
    return master;
  }

  public static OperationQueue<MasterOperation> publishMaster(InteractionProtocol protocol) {
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

  public static OperationQueue<NodeOperation> publisNode(InteractionProtocol protocol, Node node) {
    return protocol.publishNode(node, new NodeMetaData(node.getName()));
  }

  public static List<OperationQueue<NodeOperation>> publisNodes(InteractionProtocol protocol, List<Node> nodes) {
    List<OperationQueue<NodeOperation>> nodeQueues = new ArrayList<OperationQueue<NodeOperation>>();
    for (Node node : nodes) {
      nodeQueues.add(publisNode(protocol, node));
    }
    return nodeQueues;
  }

}
