/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.sf.katta.master.IPaths;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.ZkConfiguration;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

import com.yahoo.zookeeper.KeeperException;
import com.yahoo.zookeeper.Watcher;
import com.yahoo.zookeeper.ZooKeeper;
import com.yahoo.zookeeper.ZooDefs.CreateFlags;
import com.yahoo.zookeeper.ZooDefs.Ids;
import com.yahoo.zookeeper.proto.WatcherEvent;

/**
 * Abstracts the interation with zookeeper and allows permanent (not just one
 * time) watches on nodes in ZooKeeper
 * 
 */
public class ZKClient implements Watcher {

  private ZooKeeper _zk = null;

  private final Object _mutex = new Object();

  private final HashMap<String, HashSet<IZKEventListener>> _childListener = new HashMap<String, HashSet<IZKEventListener>>();

  private final HashMap<String, HashSet<IZKEventListener>> _dataListener = new HashMap<String, HashSet<IZKEventListener>>();

  private final int _port;

  private final int _timeOut;

  private final String _servers;

  public ZKClient(final ZkConfiguration configuration) {
    _servers = configuration.getZKServers();
    _port = configuration.getZKClientPort();
    _timeOut = configuration.getZKTimeOut();
    if (_zk == null) {
      try {
        _zk = new ZooKeeper(_servers, _timeOut, this);
      } catch (final Exception e) {
        _zk = null;
        throw new RuntimeException("Keeper exception when starting new session: ", e);
      }
    }
  }

  /**
   * Returns a mutex all zookeeper events are syncronized aginst. So in case you
   * need to do something without getting any zookeeper even interruption
   * syncronize against this mutex.
   */
  public Object getSyncMutex() {
    return _mutex;
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
  public ArrayList<String> subscribeChildChanges(final String path, final IZKEventListener listener)
      throws KattaException {
    addChildListener(path, listener);
    synchronized (_zk) {
      try {
        return _zk.getChildren(path, true);
      } catch (final Exception e) {
        removeChildListener(path, listener);
        throw new KattaException("unable to subscribe child changes for path: " + path, e);
      }
    }
  }

  /**
   * Subscribes notifications for permanent notifications for data changes on
   * the given node path.
   * 
   * @param path
   * @param listener
   * @throws KattaException
   */
  public void subscribeDataChanges(final String path, final IZKEventListener listener) throws KattaException {
    synchronized (_mutex) {
      _addDataListener(path, listener);
      try {
        _zk.getData(path, true, null);
      } catch (final Exception e) {
        removeDataListener(path, listener);
        throw new KattaException("Unable to subscribe data changes", e);
      }
    }
  }

  private void removeDataListener(final String path, final IZKEventListener listener) {
    final HashSet<IZKEventListener> hashSet = _dataListener.get(path);
    if (hashSet != null) {
      hashSet.remove(listener);
    }
  }

  private void removeChildListener(final String path, final IZKEventListener listener) {
    final HashSet<IZKEventListener> hashSet = _childListener.get(path);
    if (hashSet != null) {
      hashSet.remove(listener);
    }
  }

  private void addChildListener(final String path, final IZKEventListener listener) {
    if (listener != null) {
      HashSet<IZKEventListener> set = _childListener.get(path);
      if (set == null) {
        set = new HashSet<IZKEventListener>();
        _childListener.put(path, set);
      }
      set.add(listener);
    }
  }

  private void _addDataListener(final String path, final IZKEventListener listener) {
    if (listener != null) {
      HashSet<IZKEventListener> set = _dataListener.get(path);
      if (set == null) {
        set = new HashSet<IZKEventListener>();
        _dataListener.put(path, set);
      }
      set.add(listener);
    }
  }

  /**
   * Creates an node for given path without any data.
   * 
   * @param path
   * @throws KattaException
   */
  public void create(final String path) throws KattaException {
    create(path, null, 0);
  }

  /**
   * Creates an node for given path with given {@link Writable} data.
   * 
   * @param path
   * @param writable
   * @throws KattaException
   */
  public void create(final String path, final Writable writable) throws KattaException {
    create(path, writable, 0);
  }

  private void create(final String path, final Writable writable, final int flags) throws KattaException {
    assert path != null;
    final byte[] data = writableToByteArray(writable);
    synchronized (_mutex) {
      try {
        _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, flags);
      } catch (final Exception e) {
        throw new KattaException("unable to create node in ZK", e);
      }
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
    create(path, null, CreateFlags.EPHEMERAL);
  }

  /**
   * Creates and ephemeral node with given data of the writable.
   * 
   * @param path
   * @param writable
   * @throws KattaException
   */
  public void createEphemeral(final String path, final Writable writable) throws KattaException {
    create(path, writable, CreateFlags.EPHEMERAL);
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
    synchronized (_mutex) {
      try {
        if (_zk != null) {
          _zk.delete(path, -1);
        }
        return true;
      } catch (final KeeperException e) {
        String message = "unable to delete path: " + path;
        if (KeeperException.Code.NotEmpty == e.getCode()) {
          message += " Path has sub nodes.";
        }
        throw new KattaException(message, e);
      } catch (final Exception e) {
        throw new KattaException("unable to delete path:" + path, e);
      }
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
    synchronized (_mutex) {
      try {
        final ArrayList<String> children = _zk.getChildren(path, false);
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
        if (e.getCode() != KeeperException.Code.NoNode) {
          throw new KattaException("unable to delete:" + path, e);
        }
      } catch (final Exception e) {
        throw new KattaException("unable to delete:" + path, e);
      }
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
    synchronized (_mutex) {
      try {
        return _zk.exists(path, false) != null;
      } catch (final Exception e) {
        throw new KattaException("unable to check path: " + path, e);
      }
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
    synchronized (_mutex) {
      boolean watch = false;
      final HashSet<IZKEventListener> set = _childListener.get(path);
      if (set != null && set.size() > 0) {
        watch = true;
      }
      try {
        return _zk.getChildren(path, watch);
      } catch (final Exception e) {
        throw new KattaException("warn unable to retrieve children: " + path, e);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.yahoo.zookeeper.Watcher#process(com.yahoo.zookeeper.proto.WatcherEvent)
   */
  public void process(final WatcherEvent event) {
    synchronized (_mutex) {
      final String path = event.getPath();
      if (event.getType() == Watcher.Event.EventNodeChildrenChanged) {
        final HashSet<IZKEventListener> listeners = _childListener.get(path);
        if (listeners != null) {
          for (final IZKEventListener listener : listeners) {
            try {
              listener.process(event);
            } catch (final Throwable e) {
              Logger.error("Faild to process event with listener: " + listener, e);
            }
          }
          // re subscribe to event.
          try {
            _zk.getChildren(event.getPath(), true);
          } catch (final Exception e) {
            for (final IZKEventListener listener : listeners) {
              removeChildListener(path, listener);
            }
            throw new RuntimeException("Unable to re subscribe to child change notification for: " + path, e);
          }
        }
      } else if (event.getType() == Watcher.Event.EventNodeDataChanged
          || event.getType() == Watcher.Event.EventNodeDeleted) {
        final HashSet<IZKEventListener> listeners = _dataListener.get(path);
        if (listeners != null) {
          for (final IZKEventListener listener : listeners) {
            try {
              listener.process(event);
            } catch (final Throwable e) {
              Logger.error("Faild to process event with listener: " + listener, e);
            }
          }
          // re subscribe to event.
          try {
            _zk.getData(event.getPath(), true, null);
          } catch (final Exception e) {
            for (final IZKEventListener listener : listeners) {
              removeChildListener(path, listener);
            }
            throw new RuntimeException("Unable to re subscribe to data change notification.", e);
          }
        }
      } else if (event.getState() == Watcher.Event.KeeperStateExpired) {
        Logger.debug("Zookeeper session expired.");
        try {
          _zk = new ZooKeeper(_servers, _timeOut, this);
        } catch (final Exception e) {
          _zk = null;
          throw new RuntimeException("Keeper exception when starting new session: ", e);
        }
        waitForZooKeeper(30000);
        try {
          createDefaultNameSpace();
        } catch (KattaException e) {
          Logger.error("Exception on on reconnecting after session expiration.");
          throw new RuntimeException(e);
        }
      }
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
    byte[] data;
    synchronized (_mutex) {
      boolean watch = false;
      final HashSet<IZKEventListener> set = _dataListener.get(path);
      if (set != null && set.size() > 0) {
        watch = true;
      }
      try {
        data = _zk.getData(path, watch, null);
      } catch (final Exception e) {
        throw new KattaException("Failed to read data for: " + path, e);
      }
      final DataInputBuffer buffer = new DataInputBuffer();
      buffer.reset(data, data.length);
      try {
        writable.readFields(buffer);
      } catch (final IOException e) {
        throw new KattaException("unable to read data into Writable", e);
      }
    }
  }

  /**
   * Closes down the connection to zookeeper. This will remove all ephemeral
   * nodes within zookeeper this client created.
   */
  public void close() {
    synchronized (_mutex) {
      try {
        if (_zk != null) {
          try {
            _zk.disconnect();
          } catch (final IOException e) {
            throw new RuntimeException("unable to disconnect zookeeper");
          }
          _zk.close();
          _zk = null;
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException("unable to close zookeeper");
      }
    }
  }

  /**
   * Shows the full node structure of the Zookeeper server. Useful for
   * debugging.
   * 
   * @param out
   * @throws KattaException
   */
  public void showFolders() throws KattaException {
    final int level = 1;
    final StringBuffer buffer = new StringBuffer();
    final String startPath = "";
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

      addChildren(level + 1, buffer, startPath + "/" + node);
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
    synchronized (_mutex) {
      final byte[] data = writableToByteArray(writable);
      try {
        _zk.setData(path, data, -1);
      } catch (final Exception e) {
        throw new KattaException("Unable to write data for: " + path, e);
      }
    }
  }

  /**
   * StringUtil that returns the last token of an unix style path with "/" as
   * seperator.
   * 
   * @param path
   * @return
   */
  public String getNodeNameFromPath(String path) {
    assert path != null;
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    final int lastIndexOf = path.lastIndexOf("/");
    if (lastIndexOf == -1) {
      return path;
    }
    return path.substring(lastIndexOf + 1, path.length());
  }

  /**
   * Tries for given milliseconds to connect to the zookeeper server and throws
   * an {@link RuntimeException} in case that did not succeed.
   * 
   * @param ms
   */
  public void waitForZooKeeper(final long ms) {
    final long end = System.currentTimeMillis() + ms;
    try {
      while (_zk.getState() != ZooKeeper.States.CONNECTED) {
        if (end < System.currentTimeMillis()) {
          throw new RuntimeException("No connection with zookeeper server could be established.");
        }
        Logger.debug("Zookeeper ZkServer not yet available, sleeping...");
        Thread.sleep(1000);
      }
    } catch (final InterruptedException e) {
      Logger.warn("waiting for the zookeeper server was interrupted", e);
    }
  }

  /**
   * Creates a set of default folder structure for katta within a zookeeper .
   * 
   * @throws KattaException
   */
  public void createDefaultNameSpace() throws KattaException {
    Logger.debug("Creating default File structure if required....");
    if (!exists(IPaths.ROOT_PATH)) {
      create(IPaths.ROOT_PATH);
    }
    if (!exists(IPaths.INDEXES)) {
      create(IPaths.INDEXES);
    }
    if (!exists(IPaths.NODES)) {
      create(IPaths.NODES);
    }
    if (!exists(IPaths.NODE_TO_SHARD)) {
      create(IPaths.NODE_TO_SHARD);
    }
    if (!exists(IPaths.SHARD_TO_NODE)) {
      create(IPaths.SHARD_TO_NODE);
    }
  }

  public int getPort() {
    return _port;
  }

}
