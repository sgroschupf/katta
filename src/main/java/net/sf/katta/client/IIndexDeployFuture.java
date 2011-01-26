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
package net.sf.katta.client;

/**
 * 
 * Future for an index deployment.
 * 
 * @see Future for concept of a future
 */
public interface IIndexDeployFuture {

  /**
   * This method blocks until the index has been successfully deployed or the
   * deployment failed.
   */
  IndexState joinDeployment() throws InterruptedException;

  /**
   * This method blocks until the index has been successfully deployed or the
   * deployment failed or maxWaitMillis has exceeded.
   */
  IndexState joinDeployment(long maxWaitMillis) throws InterruptedException;

  IndexState getState();

}
