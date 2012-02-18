/**
 * Copyright 2009 the original author or authors.
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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.katta.util.KattaException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.log4j.Logger;

/**
 * This class is responsible for calling the sever node via an RPC proxy, and
 * possibly scheduling retires if errors occur. Only M-1 retries are attempted,
 * whereby M is the given maxRetryCount (M calls total). With replication level
 * N, there can be at most N-1 retries (N calls total).
 */
class NodeInteraction<T> implements Runnable {

  private static final Logger LOG = Logger.getLogger(NodeInteraction.class);

  // Used to make logs easier to read.
  private static int interactionInstanceCounter;

  private final Method _method;
  private final Object[] _args;
  private final int _shardArrayIndex;
  private final String _node;
  private final Map<String, List<String>> _node2ShardsMap;
  private final List<String> _shards;
  private final int _tryCount;
  private final int _maxTryCount;
  private final INodeExecutor _workQueue;
  private final INodeProxyManager _shardManager;
  private final IResultReceiver<T> _result;
  private final int instanceId = interactionInstanceCounter++;

  /**
   * Create a node interaction. This will make one call to one node, listing
   * multiple shards.
   * 
   * @param method
   *          Which method to call on the server. This method must have come
   *          from the same interface used to creat the RPC proxy in the first
   *          place (see Client constructor).
   * @param args
   *          The arguments to pass to the method. When calling the server, the
   *          shard list argument will be modified if shardArrayIndex >= 0.
   * @param shardArrayIndex
   *          Which parameter, if any, to overwrite with a String[] of shard
   *          names (this one arg then changes an a per-node basis, otherwise
   *          all arguments are the same for all calls to nodes). This is
   *          optional, depending on the needs of the server. When < 0, no
   *          overwriting is done.
   * @param node
   *          The name of the node to contact. This is used to get the node's
   *          proxy object (see IShardProxyManager).
   * @param node2ShardsMap
   *          The mapping from nodes to shards for all nodes. Used initially
   *          with the node "node", but other nodes if errors occur and we
   *          retry. For every retry the failed node is removed from the map and
   *          the map is then used to submit a retry job.
   * @param tryCount
   *          This interaction is the Nth retry (starts at 1). We use this to
   *          decide if we should retry shards.
   * @param maxTryCount
   *          The maximum number a interaction is tried to be executed.
   * @param shardManager
   *          Our source of node proxies, and access to a node selection policy
   *          to pick the nodes to use for retires. Also we notify this object
   *          on node failures.
   * @param workQueue
   *          Use this if we need to resubmit a retry job. Will result in a new
   *          NodeInteraction.
   * @result The destination to write to. If we get a result from the node we
   *         add it. If we get an error and submit retries we do not use it (the
   *         retry jobs will write to it for us). If we get an error and do not
   *         retry we write the error to it.
   */
  public NodeInteraction(Method method, Object[] args, int shardArrayIndex, String node,
          Map<String, List<String>> node2ShardsMap, int tryCount, int maxTryCount, INodeProxyManager shardManager,
          INodeExecutor workQueue, IResultReceiver<T> result) {
    _method = method;
    // Make a copy in case we will be modifying the shard list.
    _args = Arrays.copyOf(args, args.length);
    _shardArrayIndex = shardArrayIndex;
    _node = node;
    _node2ShardsMap = node2ShardsMap;
    _shards = node2ShardsMap.get(node);
    _tryCount = tryCount;
    _maxTryCount = maxTryCount;
    _workQueue = workQueue;
    _shardManager = shardManager;
    _result = result;
  }

  @SuppressWarnings("unchecked")
  public void run() {
    String methodDesc = null;
    try {
      VersionedProtocol proxy = _shardManager.getProxy(_node, false);
      if (proxy == null) {
        throw new KattaException("No proxy for node: " + _node);
      }
      if (_shardArrayIndex >= 0) {
        // We need to pass the list of shards to the server's method.
        _args[_shardArrayIndex] = _shards.toArray(new String[_shards.size()]);
      }
      long startTime = 0;
      if (LOG.isTraceEnabled()) {
        methodDesc = describeMethodCall(_method, _args, _node);
        LOG.trace(String.format("About to invoke %s using proxy %s (id=%d)", methodDesc, Proxy
                .getInvocationHandler(proxy), instanceId));
        startTime = System.currentTimeMillis();
      }
      T result = (T) _method.invoke(proxy, _args);
      _shardManager.reportNodeCommunicationSuccess(_node);
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Calling %s returned %s, took %d msec (id=%d)", methodDesc, resultToString(result),
                (System.currentTimeMillis() - startTime), instanceId));
        String methodDesc2 = describeMethodCall(_method, _args, _node);
        if (!methodDesc.equals(methodDesc2)) {
          LOG.error(String.format("Method call changed from %s to %s (id=%d)", methodDesc, methodDesc2, instanceId));
        }
      }
      _result.addResult(result, _shards);
    } catch (Throwable t) {
      // Notify the work queue, so it can mark the node as down.
      _shardManager.reportNodeCommunicationFailure(_node, t);
      if (_tryCount >= _maxTryCount) {
        LOG.error(String.format("Error calling %s (try # %d of %d) (id=%d)", (methodDesc != null ? methodDesc : _method
                + " on " + _node), _tryCount, _maxTryCount, instanceId), t);
        _result.addError(new KattaException(String.format("%s for shards %s failed (id=%d)",
                getClass().getSimpleName(), _shards, instanceId), t), _shards);
        return;
      }
      if (!_result.isClosed()) {
        try {
          // Find new node(s) for our shards and add to global node2ShardMap
          Map<String, List<String>> retryMap = _shardManager.createNode2ShardsMap(_node2ShardsMap.get(_node));
          LOG.warn(String.format("Failed to interact with node %s. Trying with other node(s) %s (id=%d)", _node,
                  retryMap.keySet(), instanceId), t);
          // Execute the action again for every node
          for (String newNode : retryMap.keySet()) {
            _workQueue.execute(newNode, retryMap, _tryCount + 1, _maxTryCount);
          }
        } catch (ShardAccessException e) {
          LOG.error(String.format("Error calling %s (try # %d of %d) (id=%d)", (methodDesc != null ? methodDesc
                  : _method + " on " + _node), _tryCount, _maxTryCount, instanceId), t);
          _result.addError(e, _shards);
        }
      }
      // We have no results to report. Submitted jobs will hopefully get results
      // instead.
    }
  }

  private String describeMethodCall(Method method, Object[] args, String nodeName) {
    StringBuilder builder = new StringBuilder(method.getDeclaringClass().getSimpleName());
    builder.append(".");
    builder.append(method.getName());
    builder.append("(");
    String sep = "";
    for (int i = 0; i < args.length; i++) {
      builder.append(sep);
      if (args[i] == null) {
        builder.append("null");
      } else if (args[i] instanceof String[]) {
        // TODO: all array types, lists, maps.
        String[] strs = (String[]) args[i];
        String sep2 = "";
        builder.append("[");
        for (String str : strs) {
          builder.append(sep2 + "\"" + str + "\"");
          sep2 = ", ";
        }
        builder.append("]");
      } else {
        builder.append(args[i].toString());
      }
      sep = ", ";
    }
    builder.append(") on ");
    builder.append(nodeName);
    return builder.toString();
  }

  private String resultToString(T result) {
    String s = "null";
    if (result != null) {
      try {
        s = result.toString();
      } catch (Throwable t) {
        LOG.trace("Error calling toString() on result", t);
        s = "(toString() err)";
      }
    }
    if (s == null) {
      s = "(null toString())";
    }
    return s;
  }

  @Override
  public String toString() {
    return "NodeInteraction: call " + _method.getName() + " on " + _node;
  }
}
