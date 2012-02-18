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
package net.sf.katta.protocol.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexMetaData implements Serializable {

  private static final long serialVersionUID = 1L;

  private String _name;
  private String _path;
  private int _replicationLevel;
  private Set<Shard> _shards = new HashSet<Shard>();
  private IndexDeployError _deployError;

  public IndexMetaData(String name, String path, int replicationLevel) {
    _name = name;
    _path = path;
    _replicationLevel = replicationLevel;
  }

  public String getPath() {
    return _path;
  }

  public void setReplicationLevel(int replicationLevel) {
    _replicationLevel = replicationLevel;
  }

  public int getReplicationLevel() {
    return _replicationLevel;
  }

  public String getName() {
    return _name;
  }

  public Set<Shard> getShards() {
    return _shards;
  }

  public Shard getShard(String shardName) {
    for (Shard shard : _shards) {
      if (shard.getName().equals(shardName)) {
        return shard;
      }
    }
    return null;
  }

  public String getShardPath(String shardName) {
    String shardPath = null;
    Shard shard = getShard(shardName);
    if (shard != null) {
      shardPath = shard.getPath();
    }
    return shardPath;
  }

  public void setDeployError(IndexDeployError deployError) {
    _deployError = deployError;
  }

  public IndexDeployError getDeployError() {
    return _deployError;
  }

  public boolean hasDeployError() {
    return _deployError != null;
  }

  @Override
  public String toString() {
    return "name: " + _name + ", replication: " + _replicationLevel + ", path: " + _path;
  }

  public static class Shard implements Serializable {

    private static final long serialVersionUID = IndexMetaData.serialVersionUID;
    private final String _name;
    private final String _path;
    private final Map<String, String> _metaDataMap = new HashMap<String, String>();

    public Shard(String name, String path) {
      _name = name;
      _path = path;
    }

    public String getName() {
      return _name;
    }

    public String getPath() {
      return _path;
    }

    public Map<String, String> getMetaDataMap() {
      return _metaDataMap;
    }

    @Override
    public String toString() {
      return getName();
    }

    public static List<String> getShardNames(Collection<Shard> shards) {
      List<String> shardNames = new ArrayList<String>(shards.size());
      for (Shard shard : shards) {
        shardNames.add(shard.getName());
      }
      return shardNames;
    }

  }

}
