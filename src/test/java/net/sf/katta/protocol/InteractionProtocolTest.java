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
package net.sf.katta.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.AbstractZkTest;
import net.sf.katta.master.Master;
import net.sf.katta.node.Node;
import net.sf.katta.node.monitor.MetricsRecord;
import net.sf.katta.operation.OperationId;
import net.sf.katta.operation.master.AbstractIndexOperation;
import net.sf.katta.operation.master.MasterOperation;
import net.sf.katta.operation.node.NodeOperation;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.testutil.Mocks;
import net.sf.katta.testutil.mockito.WaitingAnswer;
import net.sf.katta.util.ZkConfiguration.PathDef;

import org.I0Itec.zkclient.Gateway;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.util.ZkPathUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Test;
import org.mockito.InOrder;

public class InteractionProtocolTest extends AbstractZkTest {

  @Test(timeout = 7000)
  public void testLifecycle() throws Exception {
    int GATEWAY_PORT = 2190;
    Gateway gateway = new Gateway(GATEWAY_PORT, _zk.getServerPort());
    gateway.start();
    ZkClient zkClient = new ZkClient("localhost:" + GATEWAY_PORT);

    InteractionProtocol protocol = new InteractionProtocol(zkClient, _zk.getZkConf());
    final AtomicInteger connectCount = new AtomicInteger();
    final AtomicInteger disconnectCount = new AtomicInteger();
    final Object mutex = new Object();

    protocol.registerComponent(new ConnectedComponent() {
      @Override
      public void disconnect() {
        disconnectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }

      @Override
      public void reconnect() {
        connectCount.incrementAndGet();
        synchronized (mutex) {
          mutex.notifyAll();
        }
      }
    });
    synchronized (mutex) {
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
      gateway.stop();
      mutex.wait();
      gateway.start();
      mutex.wait();
    }
    zkClient.close();
    assertEquals(2, connectCount.get());
    assertEquals(2, connectCount.get());
    gateway.stop();
  }

  @Test(timeout = 7000)
  public void testNodeQueue() throws Exception {
    Node node = mock(Node.class);
    String nodeName = "node1";
    when(node.getName()).thenReturn(nodeName);

    InteractionProtocol protocol = _zk.getInteractionProtocol();
    NodeQueue nodeQueue = protocol.publishNode(node, new NodeMetaData());

    NodeOperation nodeOperation1 = mock(NodeOperation.class, withSettings().serializable().name("a"));
    NodeOperation nodeOperation2 = mock(NodeOperation.class, withSettings().serializable().name("b"));
    OperationId operation1Id = protocol.addNodeOperation(nodeName, nodeOperation1);
    OperationId operation2Id = protocol.addNodeOperation(nodeName, nodeOperation2);

    assertTrue(protocol.isNodeOperationQueued(operation1Id));
    assertTrue(protocol.isNodeOperationQueued(operation2Id));
    assertEquals(nodeOperation1.toString(), nodeQueue.remove().toString());
    assertEquals(nodeOperation2.toString(), nodeQueue.remove().toString());
    assertTrue(nodeQueue.isEmpty());
    assertFalse(protocol.isNodeOperationQueued(operation1Id));
    assertFalse(protocol.isNodeOperationQueued(operation2Id));
  }

  @Test(timeout = 7000)
  public void testPublishMaster() throws Exception {
    Master master1 = mock(Master.class);
    Master master2 = mock(Master.class);
    when(master1.getMasterName()).thenReturn("master1");
    when(master2.getMasterName()).thenReturn("master2");
    MasterOperation operation = mock(MasterOperation.class);
    _protocol.addMasterOperation(operation);

    MasterQueue queue = _protocol.publishMaster(master1);
    assertNotNull(queue);
    assertNotNull(_protocol.getMasterMD());
    assertEquals(1, queue.size());
    assertNotNull(queue.peek());

    // same again
    queue = _protocol.publishMaster(master1);
    assertNotNull(queue);
    assertNotNull(_protocol.getMasterMD());

    // second master
    queue = _protocol.publishMaster(master2);
    assertNull(queue);
    assertNotNull(_protocol.getMasterMD());
  }

  @Test(timeout = 7000)
  public void testPublishNode() throws Exception {
    Node node = Mocks.mockNode();
    assertNull(_protocol.getNodeMD(node.getName()));
    NodeQueue queue = _protocol.publishNode(node, new NodeMetaData(node.getName()));
    assertNotNull(queue);
    assertNotNull(_protocol.getNodeMD(node.getName()));

    // test operation
    NodeOperation operation = mock(NodeOperation.class);
    OperationId operationId = _protocol.addNodeOperation(node.getName(), operation);
    assertEquals(1, queue.size());
    assertNotNull(queue.peek());
    assertEquals(node.getName(), operationId.getNodeName());
  }

  @Test(timeout = 7000)
  public void testExplainStructure() throws Exception {
    _protocol.explainStructure();
    System.out.println("----------------");
    _protocol.showStructure(false);
    System.out.println("----------------");
    _protocol.showStructure(true);
  }

  @Test(timeout = 7000)
  public void testUnregisterListenersOnUnregisterComponent() throws Exception {
    ConnectedComponent component = mock(ConnectedComponent.class);
    _protocol.registerComponent(component);

    IAddRemoveListener childListener = mock(IAddRemoveListener.class);
    IZkDataListener dataListener = mock(IZkDataListener.class);
    _protocol.registerChildListener(component, PathDef.NODES_LIVE, childListener);
    _protocol.registerDataListener(component, PathDef.NODES_LIVE, "node1", dataListener);

    _zk.getZkClient().createPersistent(_zk.getZkConf().getZkPath(PathDef.NODES_LIVE, "node1"));
    Thread.sleep(500);
    verify(childListener).added("node1");
    verify(dataListener).handleDataChange(anyString(), any());
    verifyNoMoreInteractions(childListener, dataListener);

    _protocol.unregisterComponent(component);
    _zk.getZkClient().delete(_zk.getZkConf().getZkPath(PathDef.NODES_LIVE, "node1"));
    Thread.sleep(500);
    verifyNoMoreInteractions(childListener, dataListener);
    // ephemerals should be removed
    // listeners nshould be removed
  }

  @Test(timeout = 7000)
  public void testDeleteEphemeraksOnUnregisterComponent() throws Exception {
    Master master = mock(Master.class);
    _protocol.publishMaster(master);
    assertNotNull(_protocol.getMasterMD());

    _protocol.unregisterComponent(master);
    assertNull(_protocol.getMasterMD());
  }

  @Test(timeout = 7000)
  public void testChildListener() throws Exception {
    ConnectedComponent component = mock(ConnectedComponent.class);
    IAddRemoveListener listener = mock(IAddRemoveListener.class);
    PathDef pathDef = PathDef.NODES_LIVE;

    _zk.getZkClient().createPersistent(_zk.getZkConf().getZkPath(pathDef, "node1"));
    List<String> existingChilds = _protocol.registerChildListener(component, pathDef, listener);
    assertEquals(1, existingChilds.size());
    assertTrue(existingChilds.contains("node1"));

    _zk.getZkClient().createPersistent(_zk.getZkConf().getZkPath(pathDef, "node2"));
    _zk.getZkClient().delete(_zk.getZkConf().getZkPath(pathDef, "node1"));

    Thread.sleep(500);
    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).added("node2");
    inOrder.verify(listener).removed("node1");
    verifyNoMoreInteractions(listener);
  }

  @Test(timeout = 70000)
  public void testDataListener() throws Exception {
    ConnectedComponent component = mock(ConnectedComponent.class);
    IZkDataListener listener = mock(IZkDataListener.class);
    PathDef pathDef = PathDef.INDICES_METADATA;
    String zkPath = _zk.getZkConf().getZkPath(pathDef, "index1");

    Long serializable = new Long(1);
    _zk.getZkClient().createPersistent(zkPath, serializable);
    _protocol.registerDataListener(component, pathDef, "index1", listener);

    serializable = new Long(2);
    _zk.getZkClient().writeData(zkPath, serializable);
    Thread.sleep(500);
    verify(listener).handleDataChange(zkPath, serializable);

    _zk.getZkClient().delete(zkPath);
    Thread.sleep(500);
    verify(listener).handleDataDeleted(zkPath);
    verifyNoMoreInteractions(listener);
  }

  @Test(timeout = 70000)
  public void testIndexManagement() throws Exception {
    IndexMetaData indexMD = new IndexMetaData("index1", "indexPath", 2);
    indexMD.getShards().add(new Shard(AbstractIndexOperation.createShardName(indexMD.getName(), "path1"), "path1"));
    Node node = Mocks.mockNode();

    assertNull(_protocol.getIndexMD("index1"));
    assertEquals(0, _protocol.getIndices().size());

    // publish index
    _protocol.publishIndex(indexMD);
    _protocol.publishShard(node, indexMD.getShards().iterator().next().getName());
    assertNotNull(_protocol.getIndexMD("index1"));
    assertEquals(1, _protocol.getIndices().size());
    assertEquals(indexMD.getReplicationLevel(), _protocol.getIndexMD(indexMD.getName()).getReplicationLevel());

    // update index
    indexMD.setReplicationLevel(3);
    _protocol.updateIndexMD(indexMD);
    assertEquals(indexMD.getReplicationLevel(), _protocol.getIndexMD(indexMD.getName()).getReplicationLevel());

    _protocol.showStructure(false);
    _protocol.unpublishIndex(indexMD.getName());
    _protocol.showStructure(false);
    assertNull(_protocol.getIndexMD("index1"));
    assertEquals(0, _protocol.getIndices().size());

    String string = ZkPathUtil.toString(_protocol._zkClient);
    Set<Shard> shards = indexMD.getShards();
    for (Shard shard : shards) {
      assertFalse(string.contains(shard.getName()));
    }
  }

  @Test(timeout = 7000)
  public void testShardManagement() throws Exception {
    Node node1 = Mocks.mockNode();
    Node node2 = Mocks.mockNode();
    Map<String, String> shardMD = new HashMap<String, String>();
    shardMD.put("a", "1");

    assertEquals(0, _protocol.getShardNodes("shard1").size());

    // publish shard
    _protocol.publishShard(node1, "shard1");
    assertEquals(1, _protocol.getShardNodes("shard1").size());
    assertEquals(1, _protocol.getNodeShards(node1.getName()).size());
    assertEquals(0, _protocol.getNodeShards(node2.getName()).size());

    // publish shard on 2nd node
    _protocol.publishShard(node2, "shard1");
    assertEquals(2, _protocol.getShardNodes("shard1").size());
    assertEquals(1, _protocol.getNodeShards(node1.getName()).size());
    assertEquals(1, _protocol.getNodeShards(node2.getName()).size());

    // remove shard on first node
    _protocol.unpublishShard(node1, "shard1");
    assertEquals(1, _protocol.getShardNodes("shard1").size());
    assertEquals(0, _protocol.getNodeShards(node1.getName()).size());
    assertEquals(1, _protocol.getNodeShards(node2.getName()).size());

    // publish 2nd shard
    _protocol.publishShard(node1, "shard2");
    assertEquals(1, _protocol.getShardNodes("shard1").size());
    assertEquals(1, _protocol.getShardNodes("shard2").size());
    assertEquals(1, _protocol.getNodeShards(node1.getName()).size());
    assertEquals(1, _protocol.getNodeShards(node2.getName()).size());

    // remove one shard completely
    _protocol.unpublishShard(node1, "shard2");

    Map<String, List<String>> shard2NodesMap = _protocol.getShard2NodesMap(Arrays.asList("shard1"));
    assertEquals(1, shard2NodesMap.size());
    assertEquals(1, shard2NodesMap.get("shard1").size());
  }

  @Test(timeout = 7000)
  public void testMetrics() throws Exception {
    String nodeName1 = "node1";
    assertNull(_protocol.getMetric(nodeName1));
    _protocol.setMetric(nodeName1, new MetricsRecord(nodeName1));
    assertNotNull(_protocol.getMetric(nodeName1));

    String nodeName2 = "node2";
    _protocol.setMetric(nodeName2, new MetricsRecord(nodeName1));
    assertNotSame(_protocol.getMetric(nodeName1).getServerId(), _protocol.getMetric(nodeName2).getServerId());
  }

  @Test
  /**see KATTA-125*/
  public void testConcurrentModification() throws Exception {
    ConnectedComponent component1 = mock(ConnectedComponent.class);
    WaitingAnswer waitingAnswer = new WaitingAnswer();
    doAnswer(waitingAnswer).when(component1).disconnect();
    _protocol = _zk.createInteractionProtocol();
    _protocol.registerComponent(component1);
    WatchedEvent expiredEvent = new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.Expired
            .getIntValue(), null));
    _protocol.getZkClient().process(
            new WatchedEvent(new WatcherEvent(EventType.None.getIntValue(), KeeperState.SyncConnected.getIntValue(),
                    null)));
    _protocol.getZkClient().process(expiredEvent);
    // verify(component1).disconnect();

    ConnectedComponent component2 = mock(ConnectedComponent.class, "2ndComp");
    _protocol.registerComponent(component2);
    _protocol.unregisterComponent(component2);
    waitingAnswer.release();
    _protocol.disconnect();
  }
}
