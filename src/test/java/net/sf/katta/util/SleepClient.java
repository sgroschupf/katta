/**
 * Copyright 2009 the original author or authors.
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
package net.sf.katta.util;

import java.lang.reflect.Method;
import java.util.Arrays;

import net.sf.katta.client.Client;
import net.sf.katta.client.ClientResult;
import net.sf.katta.client.INodeSelectionPolicy;

import org.apache.log4j.Logger;

/**
 * The front end for a test server that just sleeps for a while then returns
 * nothing. Used for testing.
 */
public class SleepClient implements ISleepClient {

  protected final static Logger LOG = Logger.getLogger(SleepClient.class);

  private Client _client;

  public SleepClient(final INodeSelectionPolicy nodeSelectionPolicy) {
    _client = new Client(ISleepServer.class, nodeSelectionPolicy);
  }

  public SleepClient() {
    _client = new Client(ISleepServer.class);
  }

  public SleepClient(final ZkConfiguration config) {
    _client = new Client(ISleepServer.class, config);
  }

  public SleepClient(final INodeSelectionPolicy policy, final ZkConfiguration config) {
    _client = new Client(ISleepServer.class, policy, config);
  }

  public SleepClient(final INodeSelectionPolicy policy, final ZkConfiguration config,
          final ClientConfiguration clientConfiguration) {
    _client = new Client(ISleepServer.class, policy, config, clientConfiguration);
  }

  private static final Method SLEEP_METHOD;
  private static final int SLEEP_METHOD_SHARD_ARG_IDX = 2;
  static {
    try {
      SLEEP_METHOD = ISleepServer.class.getMethod("sleep", new Class[] { Long.TYPE, Integer.TYPE, String[].class });
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not find method sleep() in ISleepServer!");
    }
  }

  public int sleep(final long msec) throws KattaException {
    return sleepShards(msec, 0, null);
  }

  public int sleep(final long msec, final int delta) throws KattaException {
    return sleepShards(msec, delta, null);
  }

  public int sleepShards(final long msec, final String[] shards) throws KattaException {
    return sleepShards(msec, 0, shards);
  }

  public int sleepShards(final long msec, final int delta, final String[] shards) throws KattaException {
    ClientResult<Integer> results = _client.broadcastToShards(msec + delta + 3000, true, SLEEP_METHOD,
            SLEEP_METHOD_SHARD_ARG_IDX, shards != null ? Arrays.asList(shards) : null, msec, delta, null);
    if (results.isError()) {
      throw results.getKattaException();
    }
    int totalShards = 0;
    for (int numShards : results.getResults()) {
      totalShards += numShards;
    }
    return totalShards;
  }

  public int sleepIndices(final long msec, final String[] indices) throws KattaException {
    return sleepIndices(msec, 0, indices);
  }

  public int sleepIndices(final long msec, final int delta, final String[] indices) throws KattaException {
    ClientResult<Integer> results = _client.broadcastToIndices(msec + delta + 3000, true, SLEEP_METHOD,
            SLEEP_METHOD_SHARD_ARG_IDX, indices, msec, delta, null);
    if (results.isError()) {
      throw results.getKattaException();
    }
    int totalShards = 0;
    for (int numShards : results.getResults()) {
      totalShards += numShards;
    }
    return totalShards;
  }

  public Client getClient() {
    return _client;
  }

  public void close() {
    _client.close();
  }

}
