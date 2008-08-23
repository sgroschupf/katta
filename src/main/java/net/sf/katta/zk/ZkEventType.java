package net.sf.katta.zk;

import com.yahoo.zookeeper.Watcher;

/**
 * To improve handling of zk events, this enum is a kind adapter for the types
 * of {@link Watcher.Event}.
 * 
 */
public enum ZkEventType {

  NODE_CREATED, NODE_DELETED, NODE_DATA_CHANGED, NODE_CHILDREN_CHANGED;

  public static ZkEventType getMappedType(int zkEventType) {
    switch (zkEventType) {
    case 1:
      return NODE_CREATED;
    case 2:
      return NODE_DELETED;
    case 3:
      return NODE_DATA_CHANGED;
    case 4:
      return NODE_CHILDREN_CHANGED;
    default:
      throw new RuntimeException("type " + zkEventType + " not mapped");
    }
  }

  public static UnsupportedOperationException newUnhandledTypeException(ZkEventType eventType) {
    return new UnsupportedOperationException("unhandled type " + eventType);
  }

}
