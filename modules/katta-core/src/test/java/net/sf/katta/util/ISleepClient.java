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

/**
 * The public interface for the front end of a dummy server. It just sleeps
 * for a while then returns nothing.
 */
public interface ISleepClient {

  /**
   * Sleep for the given number of milliseconds on all nodes.
   * @param msec How long each node should sleep for.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleep(long msec) throws KattaException;
  
  /**
   * Sleep for the given number of milliseconds on all nodes.
   * @param msec How long each node should sleep for.
   * @param delta The maximum size of the delta (msec) to add or remove
   *     from the specified time. The node will choose an evenly distributed
   *     random sleep time from msec-delta to msec+delta.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleep(long msec, int delta) throws KattaException;
  
  /**
   * Sleep for the given number of milliseconds.
   * @param msec How long each node should sleep for.
   * @param shards Which shards to send the request to. Use this to control
   *     which nodes will sleep. Within a node, the shard list is ignored.
   *     The call will return after all nodes have finished sleeping.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleepShards(long msec, String[] shards) throws KattaException;
  
  /**
   * Sleep for the given number of milliseconds, +- a random delta.
   * @param msec How long each node should sleep for.
   * @param delta The maximum size of the delta (msec) to add or remove
   *     from the specified time. The node will choose an evenly distributed
   *     random sleep time from msec-delta to msec+delta.
   * @param shards Which shards to send the request to. Use this to control
   *     which nodes will sleep. Within a node, the shard list is ignored.
   *     The call will return after all nodes have finished sleeping.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleepShards(long msec, int delta, String[] shards) throws KattaException;

  /**
   * Sleep for the given number of milliseconds.
   * @param msec How long each node should sleep for.
   * @param indices Which indices to send the request to. Use this to control
   *     which nodes will sleep.
   *     The call will return after all nodes have finished sleeping.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleepIndices(long msec, String[] indices) throws KattaException;
  
  /**
   * Sleep for the given number of milliseconds, +- a random delta.
   * @param msec How long each node should sleep for.
   * @param delta The maximum size of the delta (msec) to add or remove
   *     from the specified time. The node will choose an evenly distributed
   *     random sleep time from msec-delta to msec+delta.
   * @param indices Which indices to send the request to. Use this to control
   *     which nodes will sleep.
   *     The call will return after all nodes have finished sleeping.
   * @return the total number of shards referenced (note: sleeping is done per-node).
   * @throws KattaException If an IO exception occurs.
   */
  public int sleepIndices(long msec, int delta, String[] indices) throws KattaException;

  /**
   * Closes down the client. Does nothing.
   */
  public void close();

}