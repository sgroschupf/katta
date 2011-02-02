/**
 * Copyright 2011 the original author or authors.
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

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class NodeProxyManager implements INodeProxyManager {

  private final static Logger LOG = Logger.getLogger(NodeProxyManager.class);
  private final Class<? extends VersionedProtocol> _serverClass;
  private final Configuration _hadoopConf;
  private final Map<String, VersionedProtocol> _node2ProxyMap = new ConcurrentHashMap<String, VersionedProtocol>();
  private final INodeSelectionPolicy _selectionPolicy;

  private int _successiveProxyFailuresBeforeReestablishing = 3;
  private final Multiset<String> _failedNodeInteractions = HashMultiset.create();

  public NodeProxyManager(Class<? extends VersionedProtocol> serverClass, Configuration hadoopConf,
          INodeSelectionPolicy selectionPolicy) {
    _serverClass = serverClass;
    _hadoopConf = hadoopConf;
    _selectionPolicy = selectionPolicy;
  }

  /**
   * @return how many successive proxy invocation errors must happen before the
   *         proxy is re-established.
   */
  public int getSuccessiveProxyFailuresBeforeReestablishing() {
    return _successiveProxyFailuresBeforeReestablishing;
  }

  public void setSuccessiveProxyFailuresBeforeReestablishing(int successiveProxyFailuresBeforeReestablishing) {
    _successiveProxyFailuresBeforeReestablishing = successiveProxyFailuresBeforeReestablishing;
  }

  public VersionedProtocol createNodeProxy(final String nodeName) throws IOException {
    LOG.debug("creating proxy for node: " + nodeName);

    String[] hostName_port = nodeName.split(":");
    if (hostName_port.length != 2) {
      throw new RuntimeException("invalid node name format '" + nodeName
              + "' (It should be a host name with a port number devided by a ':')");
    }
    final String hostName = hostName_port[0];
    final String port = hostName_port[1];
    final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, Integer.parseInt(port));
    VersionedProtocol proxy = RPC.getProxy(_serverClass, 0L, inetSocketAddress, _hadoopConf);
    LOG.debug(String.format("Created a proxy %s for %s:%s %s", Proxy.getInvocationHandler(proxy), hostName, port,
            inetSocketAddress));
    return proxy;
  }

  @Override
  public VersionedProtocol getProxy(String nodeName, boolean establishIfNoExists) {
    VersionedProtocol versionedProtocol = _node2ProxyMap.get(nodeName);
    if (versionedProtocol == null && establishIfNoExists) {
      synchronized (nodeName.intern()) {
        if (!_node2ProxyMap.containsKey(nodeName)) {
          try {
            versionedProtocol = createNodeProxy(nodeName);
            _node2ProxyMap.put(nodeName, versionedProtocol);
          } catch (Exception e) {
            LOG.warn("Could not create proxy for node '" + nodeName + "' - " + e.getClass().getSimpleName() + ": "
                    + e.getMessage());
          }
        }
      }
    }
    return versionedProtocol;
  }

  @Override
  public Map<String, List<String>> createNode2ShardsMap(Collection<String> shards) throws ShardAccessException {
    return _selectionPolicy.createNode2ShardsMap(shards);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void reportNodeCommunicationFailure(String nodeName, Throwable t) {
    // TODO jz: not sure if there are cases a proxy is getting invalid and
    // re-establishing it would fix the communication. If so, we should check
    // the for the exception which occurs in such cases and re-establish the
    // proxy.
    _failedNodeInteractions.add(nodeName);
    int failureCount = _failedNodeInteractions.count(nodeName);
    if (failureCount >= _successiveProxyFailuresBeforeReestablishing
            || exceptionContains(t, ConnectException.class, EOFException.class)) {
      dropNodeProxy(nodeName, failureCount);
    }
  }

  private boolean exceptionContains(Throwable t, Class<? extends Throwable>... exceptionClasses) {
    while (t != null) {
      for (Class<? extends Throwable> exceptionClass : exceptionClasses) {
        if (t.getClass().equals(exceptionClass)) {
          return true;
        }
      }
      t = t.getCause();
    }
    return false;
  }

  private void dropNodeProxy(String nodeName, int failureCount) {
    synchronized (nodeName.intern()) {
      if (_node2ProxyMap.containsKey(nodeName)) {
        LOG.warn("removing proxy for node '" + nodeName + "' after " + failureCount + " proxy-invocation errors");
        _failedNodeInteractions.remove(nodeName, Integer.MAX_VALUE);
        _selectionPolicy.removeNode(nodeName);
        VersionedProtocol proxy = _node2ProxyMap.remove(nodeName);
        RPC.stopProxy(proxy);
      }
    }
  }

  @Override
  public void reportNodeCommunicationSuccess(String node) {
    _failedNodeInteractions.remove(node, Integer.MAX_VALUE);
  }

  @Override
  public void shutdown() {
    Collection<VersionedProtocol> proxies = _node2ProxyMap.values();
    for (VersionedProtocol search : proxies) {
      RPC.stopProxy(search);
    }
  }

}
