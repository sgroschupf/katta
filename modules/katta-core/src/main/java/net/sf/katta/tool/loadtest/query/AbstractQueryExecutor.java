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
package net.sf.katta.tool.loadtest.query;

import java.io.Serializable;

import net.sf.katta.node.NodeContext;

@SuppressWarnings("serial")
public abstract class AbstractQueryExecutor<Q> implements Serializable {

  protected final String[] _indices;
  protected final Q[] _queries;

  public AbstractQueryExecutor(String[] indices, Q[] queries) {
    _indices = indices;
    _queries = queries;
  }

  public Q[] getQueries() {
    return _queries;
  }

  public String[] getIndices() {
    return _indices;
  }

  /**
   * Called from the loadtest node before calling
   * {@link #execute(NodeContext, Q)} method.
   * 
   * @param nodeContext
   * @throws Exception
   */
  public abstract void init(NodeContext nodeContext) throws Exception;

  /**
   * Called from the loadtest node after calling
   * {@link #execute(NodeContext, Q)} method the last time.
   * 
   * @param nodeContext
   * @throws Exception
   */
  public abstract void close(NodeContext nodeContext) throws Exception;

  /**
   * Might called multiple times from the loadtest node, depending on query
   * rate.
   * 
   * @param nodeContext
   * @param query
   * @throws Exception
   */
  public abstract void execute(NodeContext nodeContext, Q query) throws Exception;

}
