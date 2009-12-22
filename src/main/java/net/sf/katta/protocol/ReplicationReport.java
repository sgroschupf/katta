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

import java.util.Map;

public class ReplicationReport {

  private final int _desiredReplicationCount;
  private final int _minimalShardReplicationCount;
  private final int _maximalShardReplicationCount;
  private final Map<String, Integer> _shard2ReplicationCount;

  public ReplicationReport(Map<String, Integer> replicationCountByShardMap, int desiredReplicationCount,
          int minimalShardReplicationCount, int maximalShardReplicationCount) {
    _shard2ReplicationCount = replicationCountByShardMap;
    _desiredReplicationCount = desiredReplicationCount;
    _minimalShardReplicationCount = minimalShardReplicationCount;
    _maximalShardReplicationCount = maximalShardReplicationCount;
  }

  public int getReplicationCount(String shardName) {
    return _shard2ReplicationCount.get(shardName);
  }

  public int getDesiredReplicationCount() {
    return _desiredReplicationCount;
  }

  public int getMinimalShardReplicationCount() {
    return _minimalShardReplicationCount;
  }

  public int getMaximalShardReplicationCount() {
    return _maximalShardReplicationCount;
  }

  public boolean isUnderreplicated() {
    return getMinimalShardReplicationCount() < getDesiredReplicationCount();
  }

  public boolean isOverreplicated() {
    return getMaximalShardReplicationCount() > getDesiredReplicationCount();
  }

  public boolean isBalanced() {
    return !isUnderreplicated() && !isOverreplicated();
  }

  /**
   * @return true if each shard is deployed at least once
   */
  public boolean isDeployed() {
    return getMinimalShardReplicationCount() > 0;
  }

  @Override
  public String toString() {
    return String.format("desiredReplication: %s | minimalShardReplication: %s | maximalShardReplication: %s",
            _desiredReplicationCount, _minimalShardReplicationCount, _maximalShardReplicationCount);
  }

}
