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
package net.sf.katta.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.zk.ZKClient;

public class DefaultDistributionPolicy implements IDeployPolicy {

  /**
   * simply interate over all shards and asign them to the available slaves, no
   * replication.
   */
  public Map<String, List<AssignedShard>> ditribute(final ZKClient client, final List<String> slaves,
      final List<AssignedShard> shards) {
    if (slaves.size() == 0) {
      throw new IllegalArgumentException("no slaves");
    }
    if (shards.size() == 0) {
      throw new IllegalArgumentException("no shards");
    }

    final Map<String, List<AssignedShard>> map = new HashMap<String, List<AssignedShard>>();
    int count = 0;
    for (int i = 0; i < shards.size(); i++) {
      final AssignedShard shard = shards.get(i);
      if (slaves.size() == count) {
        count = 0;
      }
      final String slave = slaves.get(count++);
      List<AssignedShard> arrayList = map.get(slave);
      if (arrayList == null) {
        arrayList = new ArrayList<AssignedShard>();
        map.put(slave, arrayList);
      }
      arrayList.add(shard);
    }
    return map;
  }

}
