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
package net.sf.katta.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.slave.IQuery;

public class DefaultSlaveSelectionPolicy implements ISlaveSelectionPolicy {

  private Map<String, List<Map<String, List<String>>>> _slaveShardMap;

  private int _pos = 0;

  public Map<String, List<String>> getSlaveShardsMap(final IQuery query, final String[] indexNames) {
    if (_slaveShardMap == null) {
      throw new IllegalArgumentException("no index deployed yet, try later again...");
    }
    final HashMap<String, List<String>> map = new HashMap<String, List<String>>();
    for (final String indexName : indexNames) {
      final List<Map<String, List<String>>> options = _slaveShardMap.get(indexName);
      if (options == null) {
        throw new IllegalArgumentException("no index deployed yet, try later again...");
      }
      if (_pos >= options.size()) {
        _pos = 0;
      }

      final Map<String, List<String>> oneOptionForIndex = options.get(_pos);

      final Set<String> salves = oneOptionForIndex.keySet();
      for (final String slave : salves) {
        List<String> arrayList = map.get(slave);
        if (arrayList == null) {
          arrayList = new ArrayList<String>();
          map.put(slave, arrayList);
        }
        arrayList.addAll(oneOptionForIndex.get(slave));
      }
    }
    _pos++;
    return map;
  }

  public void setShardsAndSlaves(final Map<String, List<String>> indexToShards,
      final Map<String, List<String>> shardsToSlave) {
    _slaveShardMap = computeMap(indexToShards, shardsToSlave);
  }

  private Map<String, List<Map<String, List<String>>>> computeMap(final Map<String, List<String>> indexToShards,
      final Map<String, List<String>> shardsToSlave) {
    if (indexToShards.size() == 0 || shardsToSlave.size() == 0) {
      throw new IllegalArgumentException("IndexToShards or shardsToSlave can't be empthy.");
    }
    // a list of slave to shards for each index..
    final Map<String, List<Map<String, List<String>>>> result = new HashMap<String, List<Map<String, List<String>>>>();
    // all indexes we known
    final Set<String> indexNames = indexToShards.keySet();

    // we want to create as many slaveToShard lists as we have slaves
    // serving a given shard.
    for (final String indexName : indexNames) {
      int pos = 0;
      while (true) {

        // the shards we need to server for a given index.
        final List<String> requiredShards = indexToShards.get(indexName);
        // in case we have a corruped index with 0 shards..

        if (requiredShards.size() == 0) {
          break;
        }
        // the slaves to shard map, where we collect the different
        // shards a slave has to search in..
        final HashMap<String, List<String>> slaveToShardMap = new HashMap<String, List<String>>();
        boolean newSet = true;
        for (final String shard : requiredShards) {
          // now we pic one slave base on our position pos. Pos will
          // be incremented with each while loop.
          final List<String> slaves = shardsToSlave.get(shard);
          if (pos == slaves.size()) {
            newSet = false;
            break;
          }
          final String slave = slaves.get(pos);
          // add the shard to the list of shards the slave have to
          // search in
          List<String> slaveShards = slaveToShardMap.get(slave);
          if (slaveShards == null) {
            slaveShards = new ArrayList<String>();
            slaveToShardMap.put(slave, slaveShards);
          }
          slaveShards.add(shard);
        }
        if (newSet) {
          // add the new generated slaveToShard map to all our maps..
          List<Map<String, List<String>>> arrayList = result.get(indexName);
          if (arrayList == null) {
            arrayList = new ArrayList<Map<String, List<String>>>();
            result.put(indexName, arrayList);
          }
          // this should be repeated until we have as much as replica
          // we have..
          arrayList.add(slaveToShardMap);
          pos++;
        } else {
          break;
        }
      }
    }
    return result;
  }

}
