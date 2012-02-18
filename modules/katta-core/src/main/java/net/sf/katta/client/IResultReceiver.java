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
package net.sf.katta.client;

import java.util.Collection;

/**
 * These are the only ClientResult methods NodeInteraction is allowed to call.
 */
public interface IResultReceiver<T> {

  /**
   * @return true if the result is closed, and therefore not accepting any new
   *         results.
   */
  public boolean isClosed();

  /**
   * Add the shard's results. Silently fails if result is closed.
   * 
   * @param result
   *          The result to add.
   * @param shards
   *          The shards that were called to produce the result.
   */
  public void addResult(T result, Collection<String> shards);

  /**
   * Report an error thrown by the node when we tried to access the specified
   * shards. Silently fails if result is closed.
   * 
   * @param result
   *          The result to add.
   * @param shards
   *          The shards that were called to produce the result.
   */
  public void addError(Throwable error, Collection<String> shards);

}
