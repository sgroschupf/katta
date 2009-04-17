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
package net.sf.katta.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.proto.WatcherEvent;

/**
 * Abstracts the interation with zookeeper and allows permanent (not just one
 * time) watches on nodes in ZooKeeper
 * 
 */
public class ZKClient implements Watcher {

  private final static Logger LOG = Logger.getLogger(ZKClient.class);

  private ZooKeeper _zk = null;
  private final ZkLock _zkEventLock = new ZkLock();

  private final Map<String, Set<IZkChildListener>> _path2ChildListenersMap = new ConcurrentHashMap<String, Set<IZkChildListener>>();
  private final Map<String, Set<IZkDataListener>> _path2DataListenersMap = new ConcurrentHashMap<String, Set<IZkDataListener>>();
  private final Set<IZkReconnectListener> _reconnectListener = new CopyOnWriteArraySet<IZkReconnectListener>();

  private final String _servers;
  private final int _port;
  private final int _timeOut;
  private boolean _shutdownTriggered;

  public ZKClient(final ZkConfiguration configuration) {
    _servers = configuration.getZKServers();
    _port = configuration.getZKClientPort();
    _timeOut = configuration.getZKTimeOut();
  }

  public boolean isStarted() {
    return _zk != null;
  }

  /**
   * Starts a zookeeper client and waits until connected with one of the
   * zookeeper servers.
   * 
   * @param maxMsToWaitUntilConnected
   *          the milliseconds a method call will wait until the zookeeper
   *          client is connected with the server
   * @throws KattaException
   *           if connection fails or default namespaces could not be created
   */
  public void start(final long maxMsToWaitUntilConnected) throws KattaException {
    if (_zk != null) {
      throw new IllegalStateException("zk client has already been started");
    }

    try {
      getEventLock().lock();
      _shutdownTriggered = false;
      final long startTime = System.currentTimeMillis();
      try {
        _zk = new ZooKeeper(_servers, _timeOut, this);
      } catch (final Exception e) {
        _zk = null;
        throw new KattaException("Could not start zookeeper service", e);
      }

      // wait until connected
      try {
        while (_zk.getState() == ZooKeeper.States.CONNECTING) {
          ZooKeeper.States state = _zk.getState();
          if (System.currentTimeMillis() - startTime > maxMsToWaitUntilConnected) {
            _zk = null;
            throw new KattaException("No connected with zookeeper server yet. Current state is " + state);
          }
          LOG.debug("Zookeeper ZkServer not yet available, sleeping...");
          getEventLock().getStateChangedCondition().await(1000, TimeUnit.MILLISECONDS);
          // Thread.sleep(1000);
        }

        // createDefaultNameSpace();
      } catch (final InterruptedException e) {
        LOG.warn("waiting for the zookeeper server was interrupted", e);
      }
    } finally {
      getEventLock().unlock();
    }
  }

  /**
   * Closes down the connection to zookeeper. This will remove all ephemeral
   * nodes within zookeeper this client created.
   */
  public void close() {
    getEventLock().lock();
    try {
      _shutdownTriggered = true;
      if (_zk != null) {
        _zk.close();
        _zk = null;
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException("unable to close zookeeper");
    } finally {
      getEventLock().unlock();
    }
  }

  private void ensureZkRunning() {
    if (_shutdownTriggered) {
      throw new IllegalStateException("zk client is already closing");
    }
    if (_zk == null) {
      throw new IllegalStateException("zk client has not been started yet");
    }
  }

  /**
   * Returns a mutex all zookeeper events are synchronized aginst. So in case
   * you need to do something without getting any zookeeper event interruption
   * synchronize against this mutex. Also all threads waiting on this mutex
   * object will be notified on an event.
   */
  public ZkLock getEventLock() {
    return _zkEventLock;
  }

  /**
   * Subscribes an {@link IZKEventListener} for permanent notifications for
   * children changes (adds and removes) of given path.
   * 
   * @param path
   * @param listener
   * @return list of children nodes for given path.
   * @throws KattaException
   *           Thrown in case we can't read the children nodes. Note that we
   *           also remove the notification listener.
   */
  public List<String> subscribeChildChanges(final String path, final IZkChildListener listener) throws KattaException {
    ensureZkRunning();
    addChildListener(path, listener);
    try {
      return _zk.getChildren(path, true);
    } catch (final Exception e) {
      removeChildListener(path, listener);
      throw new KattaException("unable to subscribe child changes for path: " + path, e);
    }
  }

  public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
    removeChildListener(path, childListener);
  }

  public void unsubscribeDataChanges(String path, IZkDataListener<?> dataListener) {
    removeDataListener(path, dataListener);
  }

  public void subscribeReconnects(final IZkReconnectListener listener) {
    _reconnectListener.add(listener);
  }

  /**
   * Subscribes notifications for permanent notifications for data changes on
   * the given node path.
   * 
   * @param path
   * @param listener
   * @throws KattaException
   */
  public void subscribeDataChanges(final String path, final IZkDataListener listener) throws KattaException {
    ensureZkRunning();
    addDataListener(path, listener);
    try {
      _zk.getData(path, true, null);
    } catch (final Exception e) {
      removeDataListener(path, listener);
      throw new KattaException("Unable to subscribe data changes for path: " + path, e);
    }
  }

  /**
   * Unsubscribe all listeners from zk events.
   */
  public void unsubscribeAll() {
    _path2ChildListenersMap.clear();
    _path2DataListenersMap.clear();
    _reconnectListener.clear();
  }

  private void removeDataListener(final String path, final IZkDataListener listener) {
    ensureZkRunning();
    final Set<IZkDataListener> listeners = _path2DataListenersMap.get(path);
    if (listeners != null) {
      listeners.remove(listener);
    }
  }

  private void removeChildListener(final String path, final IZkChildListener listener) {
    ensureZkRunning();
    final Set<IZkChildListener> listeners = _path2ChildListenersMap.get(path);
    if (listeners != null) {
      listeners.remove(listener);
    }
  }

  private void addChildListener(final String path, final IZkChildListener listener) {
    ensureZkRunning();
    Set<IZkChildListener> listeners;
    synchronized (_path2ChildListenersMap) {
      listeners = _path2ChildListenersMap.get(path);
      if (listeners == null) {
        listeners = new CopyOnWriteArraySet<IZkChildListener>();
        _path2ChildListenersMap.put(path, listeners);
      }
    }
    listeners.add(listener);
  }

  private void addDataListener(final String path, final IZkDataListener listener) {
    ensureZkRunning();
    Set<IZkDataListener> listeners;
    synchronized (_path2DataListenersMap) {
      listeners = _path2DataListenersMap.get(path);
      if (listeners == null) {
        listeners = new CopyOnWriteArraySet<IZkDataListener>();
        _path2DataListenersMap.put(path, listeners);
      }
    }
    listeners.add(listener);
  }

  /**
   * Creates an node for given path without any data.
   * 
   * @param path
   * @throws KattaException
   */
  public void create(final String path) throws KattaException {
    create(path, null, CreateMode.PERSISTENT);
  }

  /**
   * Creates an node for given path with given {@link Writable} data.
   * 
   * @param path
   * @param writable
   * @throws KattaException
   */
  public void create(final String path, final Writable writable) throws KattaException {
    create(path, writable, CreateMode.PERSISTENT);
  }

  private void create(final String path, final Writable writable, CreateMode mode) throws KattaException {
    ensureZkRunning();
    assert path != null;
    final byte[] data = writableToByteArray(writable);
    try {
      _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    } catch (final Exception e) {
      throw new KattaException("unable to create path '" + path + "' in ZK", e);
    }
  }

  private byte[] writableToByteArray(final Writable writable) throws KattaException {
    byte[] data = new byte[0];
    if (writable != null) {
      final DataOutputBuffer out = new DataOutputBuffer();
      try {
        writable.write(out);
      } catch (final IOException e) {
        throw new KattaException("unable to serialize writable", e);
      }
      data = out.getData();
    }
    return data;
  }

  /**
   * Creates an ephemeral node for give path. In case the client that created
   * that node disconnects the node is removed.
   * 
   * @param path
   * @throws KattaException
   */
  public void createEphemeral(final String path) throws KattaException {
    create(path, null, CreateMode.EPHEMERAL);
  }

  /**
   * Creates and ephemeral node with given data of the writable.
   * 
   * @param path
   * @param writable
   * @throws KattaException
   */
  public void createEphemeral(final String path, final Writable writable) throws KattaException {
    create(path, writable, CreateMode.EPHEMERAL);
  }

  /**
   * Deletes a given path. For recursive deletes use
   * {@link #deleteRecursive(String)}.
   * 
   * @param path
   * @return
   * @throws KattaException
   */
  public boolean delete(final String path) throws KattaException {
    ensureZkRunning();
    try {
      if (_zk != null) {
        _zk.delete(path, -1);
      }
      return true;
    } catch (final KeeperException e) {
      String message = "unable to delete path: " + path;
      if (KeeperException.Code.NOTEMPTY == e.code()) {
        message += " Path has sub nodes.";
      }
      throw new KattaException(message, e);
    } catch (final Exception e) {
      throw new KattaException("unable to delete path:" + path, e);
    }
  }

  public void deleteIfExists(String path) throws KattaException {
    if (exists(path)) {
      delete(path);
    }
  }

  /**
   * Deletes a path and all children recursivly.
   * 
   * @param path
   * @return
   * @throws KattaException
   */
  public boolean deleteRecursive(final String path) throws KattaException {
    ensureZkRunning();
    try {
      final List<String> children = _zk.getChildren(path, false);
      for (final String subPath : children) {
        if (!deleteRecursive(path + "/" + subPath)) {
          return false;
        }
      }
    } catch (final KeeperException e) {
      // do nothing since we simply do not have children here.
    } catch (final InterruptedException e) {
      throw new KattaException("retrieving children was interruppted", e);
    }

    try {
      _zk.delete(path, -1);
    } catch (final KeeperException e) {
      if (e.code() != KeeperException.Code.NONODE) {
        throw new KattaException("unable to delete:" + path, e);
      }
    } catch (final Exception e) {
      throw new KattaException("unable to delete:" + path, e);
    }
    return true;
  }

  /**
   * Checks if a node with given path exists.
   * 
   * @param path
   * @return
   * @throws KattaException
   */
  public boolean exists(final String path) throws KattaException {
    ensureZkRunning();
    try {
      return _zk.exists(path, false) != null;
    } catch (final Exception e) {
      throw new KattaException("unable to check path: " + path, e);
    }
  }

  /**
   * Returns an List of all Children names of given path.
   * 
   * @param path
   * @return
   * @throws KattaException
   */
  public List<String> getChildren(final String path) throws KattaException {
    ensureZkRunning();
    boolean watch = false;
    final Set<IZkChildListener> listeners = _path2ChildListenersMap.get(path);
    if (listeners != null && listeners.size() > 0) {
      watch = true;
    }
    try {
      return _zk.getChildren(path, watch);
    } catch (final Exception e) {
      throw new KattaException("warn unable to retrieve children: " + path, e);
    }
  }

  public int countChildren(String path) throws KattaException {
    ensureZkRunning();
    int childCount = 0;
    if (exists(path)) {
      childCount = getChildren(path).size();
    }
    return childCount;
  }

  @Override
  public void process(WatchedEvent event) {

    // public void process(final WatcherEvent event) {
    // if (null == event.getPath()) {
    // // prohibit nullpointer (See ZOOKEEPER-77)
    // event.setPath("null");
    // }
    boolean stateChanged = event.getState() == KeeperState.Disconnected || event.getState() == KeeperState.Expired;
    boolean dataChanged = event.getType() == Watcher.Event.EventType.NodeDataChanged
            || event.getType() == Watcher.Event.EventType.NodeChildrenChanged ||  event.getType() == Watcher.Event.EventType.NodeDeleted;
    try {
      getEventLock().lock();
      if (_shutdownTriggered) {
        LOG.debug("ignoring event '{" + event.getType() + " | " + event.getPath() + "}' since shutdown triggered");
        return;
      }
      if (stateChanged) {
        processDisconnect(event);
      }
      if (dataChanged) {
        processDataOrChildChange(event);
      }
    } finally {
      if (stateChanged) {
        getEventLock().getStateChangedCondition().signalAll();
      }
      if (dataChanged) {
        getEventLock().getDataChangedCondition().signalAll();
      }
      getEventLock().unlock();
    }
  }

  private void processDisconnect(WatchedEvent event) {
    // we do a reconnect
    LOG.warn("disconnected from zookeeper (" + event.getState() + ")");
    if (_shutdownTriggered) {
      // already closing
    } else {
      reconnect();
    }
  }

  private void processDataOrChildChange(WatchedEvent event) {
    // ZkEventType eventType = ZkEventType.getMappedType(event.getType());
    final String path = event.getPath();

    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      Set<IZkChildListener> childListeners = _path2ChildListenersMap.get(path);
      if (childListeners != null && !childListeners.isEmpty()) {
        // since resubscribeChildPath might remove listener from listeners
        // because they throw exeception we need to copy the listener first to
        // make sure we also notify listener we could not re subscribe to the
        // given path
        HashSet<IZkChildListener> copiedSet = new HashSet<IZkChildListener>();
        copiedSet.addAll(childListeners);

        List<String> children = resubscribeChildPath(event, path, childListeners);

        for (final IZkChildListener listener : copiedSet) {
          try {
            listener.handleChildChange(event.getPath(), children);
          } catch (final Throwable e) {
            LOG.error("Faild to process event with listener: " + listener, e);
          }
        }
      }
    } else {
      Set<IZkDataListener> listeners = _path2DataListenersMap.get(path);
      if (listeners != null && !listeners.isEmpty()) {

        HashSet<IZkDataListener> copiedSet = new HashSet<IZkDataListener>();
        copiedSet.addAll(listeners);
        byte[] data = resubscribeDataPath(event, path, listeners);

        for (final IZkDataListener listener : copiedSet) {
          try {
            if (event.getType() == Watcher.Event.EventType.NodeCreated) {
              listener.handleDataAdded(event.getPath(), readWritable(listener.createWritable(), data));
            } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
              listener.handleDataChange(event.getPath(), readWritable(listener.createWritable(), data));
            } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
              listener.handleDataDeleted(event.getPath());
            } else {
              LOG.error("Received a unknown event, ignoring: " + event.getType());
            }
          } catch (final Throwable e) {
            LOG.error("Faild to process event " + event.getType() + " with listener: " + listener, e);
          }
        }
      }
    }
  }

  private byte[] resubscribeDataPath(WatchedEvent event, final String path, final Set<IZkDataListener> listeners) {
    byte[] data = null;
    try {
      data = _zk.getData(event.getPath(), true, null);
    } catch (final Exception e) {
      for (final IZkDataListener listener : listeners) {
        removeDataListener(path, listener);
      }
      LOG.fatal("resubscription for data changes on path '" + path + "' failed. removing listeners", e);
    }
    return data;
  }

  private List<String> resubscribeChildPath(WatchedEvent event, final String path,
          final Set<IZkChildListener> childListeners) {
    List<String> children;
    try {
      children = _zk.getChildren(event.getPath(), true);
    } catch (final Exception e) {
      LOG.fatal("re-subscription for child changes on path '" + path + "' failed. removing listeners", e);
      children = Collections.EMPTY_LIST;
      // childListeners.clear();
    }
    return children;
  }

  private void reconnect() {
    try {
      close();
      start(1000 * 60 * 10);
      for (IZkReconnectListener reconnectListener : _reconnectListener) {
        reconnectListener.handleReconnect();
      }
    } catch (final Exception e) {
      throw new RuntimeException("Exception while restarting zk client", e);
    }
  }

  /**
   * Reads that data of given path into a writeable instance. Make sure you use
   * the same writable implementation as you used to write the data.
   * 
   * @param path
   * @param writable
   * @return
   * @throws KattaException
   */
  public void readData(final String path, final Writable writable) throws KattaException {
    ensureZkRunning();
    byte[] data;
    boolean watch = false;
    final Set<IZkDataListener> set = _path2DataListenersMap.get(path);
    if (set != null && set.size() > 0) {
      watch = true;
    }
    try {
      data = _zk.getData(path, watch, null);
    } catch (final Exception e) {
      throw new KattaException("Failed to read data for: " + path, e);
    }
    readWritable(writable, data);
  }

  public <T extends Writable> T readData(final String path, final Class<T> writableClass) throws KattaException {
    T newInstance;
    try {
      newInstance = writableClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("could not create instance of " + writableClass.getName(), e);
    }
    readData(path, newInstance);
    return newInstance;
  }

  private Writable readWritable(final Writable writable, byte[] data) throws KattaException {
    final DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(data, data.length);
    try {
      writable.readFields(buffer);
    } catch (final IOException e) {
      throw new KattaException("unable to read data into Writable", e);
    } finally {
      try {
        buffer.close();
      } catch (IOException e) {
        LOG.warn("could not close data buffer", e);
      }
    }
    return writable;
  }

  /**
   * Shows the full node structure of the Zookeeper server. Useful for
   * debugging.
   * 
   * @throws KattaException
   */
  public void showFolders() throws KattaException {
    final int level = 1;
    final StringBuffer buffer = new StringBuffer();
    final String startPath = "/";
    addChildren(level, buffer, startPath);
    try {
      System.out.write(buffer.toString().getBytes());
    } catch (final IOException e) {
      e.printStackTrace();
    }

  }

  private void addChildren(final int level, final StringBuffer buffer, final String startPath) throws KattaException {
    final List<String> children = getChildren(startPath);
    for (final String node : children) {
      buffer.append(getSpaces(level - 1) + "'-" + "+" + node + "\n");

      addChildren(level + 1, buffer, (startPath + "/" + node).replaceAll("//", "/"));
    }
  }

  private String getSpaces(final int level) {
    String s = "";
    for (int i = 0; i < level; i++) {
      s += "  ";
    }
    return s;
  }

  /**
   * Creates a node and writes data of writable into the given path.
   * 
   * @param path
   * @param writable
   * @throws KattaException
   */
  public void writeData(final String path, final Writable writable) throws KattaException {
    ensureZkRunning();
    final byte[] data = writableToByteArray(writable);
    try {
      _zk.setData(path, data, -1);
    } catch (final Exception e) {
      throw new KattaException("Unable to write data for: " + path, e);
    }
  }

  public int getPort() {
    return _port;
  }

  public ZooKeeper.States getZookeeperState() {
    return _zk != null ? _zk.getState() : null;
  }

  // **************************************************
  // ********** katta specific methods
  // **************************************************

  /**
   * Creates a set of default folder structure for katta within a zookeeper .
   * 
   * @throws KattaException
   */
  public void createDefaultNameSpace() throws KattaException {
    LOG.debug("Creating default File structure if required....");
    try {
      if (!exists(ZkPathes.ROOT_PATH)) {
        create(ZkPathes.ROOT_PATH);
      }
      if (!exists(ZkPathes.INDEXES)) {
        create(ZkPathes.INDEXES);
      }
      if (!exists(ZkPathes.NODES)) {
        create(ZkPathes.NODES);
      }
      if (!exists(ZkPathes.NODE_TO_SHARD)) {
        create(ZkPathes.NODE_TO_SHARD);
      }
      if (!exists(ZkPathes.SHARD_TO_NODE)) {
        create(ZkPathes.SHARD_TO_NODE);
      }
      if (!exists(ZkPathes.SHARD_TO_ERROR)) {
        create(ZkPathes.SHARD_TO_ERROR);
      }
    } catch (KattaException e) {
      if (e.getCause() instanceof KeeperException && e.getCause().getMessage().contains("KeeperErrorCode = NodeExists")) {
        // if components starting concurrently the exists is not safe
        LOG.debug("could not create default namespace " + e.getCause().getMessage());
      } else {
        throw e;
      }
    }
  }

  /**
   * @return all nodes known to the system, also if currently disconnected
   */
  public List<String> getKnownNodes() throws KattaException {
    return getChildren(ZkPathes.NODE_TO_SHARD);
  }

  /**
   * @return all nodes connected to th zk system
   */
  public List<String> getAliveNodes() throws KattaException {
    return getChildren(ZkPathes.NODES);
  }

  public static class ZkLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;

    private Condition _dataChangedCondition = newCondition();
    private Condition _stateChangedCondition = newCondition();

    /**
     * This condition will be signaled if a zookeeper event was processed and
     * the event contains a data/child change.
     */
    public Condition getDataChangedCondition() {
      return _dataChangedCondition;
    }

    /**
     * This condition will be signaled if a zookeeper event was processed and
     * the event contains a state change (connected, disconnected, session
     * expired, etc ...).
     */
    public Condition getStateChangedCondition() {
      return _stateChangedCondition;
    }

  }

}
