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
package net.sf.katta.master;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.testutil.PrintMethodNames;
import net.sf.katta.testutil.ZkTestSystem;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class IndexStateListenerTest {

  // o test DistributeShardsThread overall ?
  // o IndexStateListener:
  // oo implement IZkStateListener (see IndexDeployFuture) o handle changing
  // oo handle nodeCount ?
  // oo none-zk model ? (Master - zk binding)
  // o let this issue sleep until hang occurs
  @Rule
  public ZkTestSystem _zk = ZkTestSystem.getInstance();
  @Rule
  public PrintMethodNames _printMethodNames = new PrintMethodNames();

  @Test
  @Ignore
  public void testit() throws Exception {
    int nodeCount = 3;
    int replicationLevel = 2;
    String indexName = "index";
    IndexMetaData indexMD = new IndexMetaData(indexName, "/virtual-index", replicationLevel, IndexState.ANNOUNCED);
    Set<String> shards = new HashSet<String>(Arrays.asList("shard1", "shard2", "shard3"));

    // DistributeShardsThread.IndexStateListener indexStateListener = new
    // DistributeShardsThread(_zk.getZkConf(), _zk
    // .getZkClient(), new DefaultDistributionPolicy(), 500).new
    // IndexStateListener(indexName, indexMD, shards,
    // nodeCount);
    // indexStateListener.subscribeShardEvents();
  }
}
