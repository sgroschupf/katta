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
