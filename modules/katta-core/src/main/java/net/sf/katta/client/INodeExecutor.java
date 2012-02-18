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

import java.util.List;
import java.util.Map;

/**
 * How a NodeInteraction resubmits jobs to the WorkQueue to retry failed nodes.
 */
interface INodeExecutor {

  /**
   * Submit a new job, which will result in a new NodeInteraction. This is used
   * to resubmit jobs to the WorkQueue, to try to get results for failed shards
   * an alternate nodes.
   * 
   * @param node
   *          The node to call.
   * @param nodeShardMap
   *          The entire node to shard map to use. This may have been reduced
   *          one or two times from the original version from the Client,
   *          depending on errors and errors on retries.
   * @param tryCount
   *          This job would be the Nth retry. Starts at 1. The NodeInteraction
   *          uses this to decide whether or not to retry (M tries max).
   * @param maxTryCount
   *          The maximum nuber of retries for a given interaction.
   */
  public void execute(String node, Map<String, List<String>> nodeShardMap, int tryCount, int maxTryCount);

}
