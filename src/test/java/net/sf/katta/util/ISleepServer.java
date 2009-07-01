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

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * The public interface for the back end of a dummy server that just
 * sleeps for a while then returns null. Used for testing.
 */
public interface ISleepServer extends VersionedProtocol {
  
  /**
   * Sleep for the given number of milliseconds, +- a random delta.
   * @param msec How long each node should sleep for.
   * @param delta The maximum size of the delta (msec) to add or remove
   *     from the specified time. The node will choose an evenly distributed
   *     random sleep time from msec-delta to msec+delta.
   * @param shards Which shards to use. This is only for testing. The sleep
   *     happens on a per-node basis. If invalid shards are passed in, an
   *     IllegalArgumentException is thrown. Otherwise this parameter is ignored.
   *     No checking is done if the value is null.
   * @return the number of shards used (note: sleeping is only done once).
   * @throws IllegalArgumentException if invalid shard names are passed in.
   */
  public int sleep(long msec, int delta, String[] shards) throws IllegalArgumentException;

}
