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
package net.sf.katta.index;

import java.io.Serializable;

public class AssignedShard implements Serializable {

  private String _indexName;
  private String _shardPath;

  public AssignedShard() {
    // for serialization
  }

  public AssignedShard(final String indexName, final String shardPath) {
    _indexName = indexName;
    _shardPath = shardPath;
  }

  public String getShardName() {
    return _indexName + "_" + getLastNodeName();
  }

  private String getLastNodeName() {
    final String shardPath = getShardPath();
    int lastIndexOf = shardPath.lastIndexOf("/");
    if (lastIndexOf == -1) {
      lastIndexOf = 0;
    }
    String name = shardPath.substring(lastIndexOf + 1, shardPath.length());
    if (name.endsWith(".zip")) {
      name = name.substring(0, name.length() - 4);
    }
    return name;
  }

  public String getShardPath() {
    return _shardPath;
  }

  public String getIndexName() {
    return _indexName;
  }

  @Override
  public String toString() {
    return getShardName();
  }
}
