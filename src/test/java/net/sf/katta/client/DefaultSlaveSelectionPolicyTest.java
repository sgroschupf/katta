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

import junit.framework.TestCase;
import net.sf.katta.node.Query;

public class DefaultSlaveSelectionPolicyTest extends TestCase {

  public void testSelection() throws Exception {
    final DefaultSlaveSelectionPolicy policy = new DefaultSlaveSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    final List<String> shards = new ArrayList<String>();
    shards.add("shardA");
    shards.add("shardB");
    indexToShards.put("indexA", shards);
    indexToShards.put("indexA", shards);

    final Map<String, List<String>> shardsToSlave = new HashMap<String, List<String>>();
    final List<String> slaves = new ArrayList<String>();
    slaves.add("slave1");
    slaves.add("slave2");
    shardsToSlave.put("shardA", slaves);
    shardsToSlave.put("shardB", slaves);

    policy.setShardsAndSlaves(indexToShards, shardsToSlave);
    Map<String, List<String>> slaveShardsMap = policy.getSlaveShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(slaveShardsMap);
    slaveShardsMap = policy.getSlaveShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(slaveShardsMap);
  }

  public void testSetShardsAndSlaves() throws Exception {
    final DefaultSlaveSelectionPolicy policy = new DefaultSlaveSelectionPolicy();
    final Map<String, List<String>> indexToShards = new HashMap<String, List<String>>();
    final List<String> shards = new ArrayList<String>();
    shards.add("shardA");
    shards.add("shardB");
    indexToShards.put("indexA", shards);

    final Map<String, List<String>> shardsToSlave = new HashMap<String, List<String>>();
    final List<String> slaves = new ArrayList<String>();
    slaves.add("slave1");
    slaves.add("slave2");
    shardsToSlave.put("shardA", slaves);
    shardsToSlave.put("shardB", slaves);

    policy.setShardsAndSlaves(indexToShards, shardsToSlave);
    Map<String, List<String>> slaveShardsMap = policy.getSlaveShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(slaveShardsMap);
    slaveShardsMap = policy.getSlaveShardsMap(new Query(), new String[] { "indexA" });
    System.out.println(slaveShardsMap);
  }

}
