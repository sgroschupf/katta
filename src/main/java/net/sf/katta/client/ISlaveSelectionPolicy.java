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

import java.util.List;
import java.util.Map;

import net.sf.katta.slave.IQuery;

/**
 * Returns a Map with Slaves and shards within those slaves that have to be
 * searched by the client.
 * 
 * Since shards can be replicated over different slaves and slaves can be
 * distributed in different network sections (same switch or rack, same data
 * center etc.) we allow custom selection policies to implement the logic to
 * make the smartest possible choice of slaves the client has to query. The
 * slave selection policy is also the place where an load balancing schema need
 * to be implemented.
 */
public interface ISlaveSelectionPolicy {

  /**
   * During startup or as soon the client get a notification about any change in
   * the gird the client sets the indexToShards and shcardsToSlave maps. An
   * {@link ISlaveSelectionPolicy} should try to precompute as much as possible
   * in this method.
   * 
   * @param indexToShards
   * @param shardsToSlave
   */
  public void setShardsAndSlaves(Map<String, List<String>> indexToShards, Map<String, List<String>> shardsToSlave);

  /**
   * Returns a map where as key the slaveName is used and as value a list shards
   * served by slave we need to query. Ideally this method returns slaves with
   * low latency to the client and alternate between slaves to load balance high
   * traffic.
   * 
   * @param queries
   * @param indexNames
   * @return
   */
  public Map<String, List<String>> getSlaveShardsMap(IQuery queries, String[] indexNames);

}
