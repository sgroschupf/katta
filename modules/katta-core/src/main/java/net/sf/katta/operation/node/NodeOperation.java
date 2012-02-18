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
package net.sf.katta.operation.node;

import java.io.Serializable;

import net.sf.katta.node.NodeContext;

/**
 * An operation executed by an {@link Node}.
 */
public interface NodeOperation extends Serializable {

  /**
   * @param context
   * @return null or an {@link OperationResult}
   * @throws InterruptedException
   */
  OperationResult execute(NodeContext context) throws InterruptedException;
}
