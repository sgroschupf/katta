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
package net.sf.katta.protocol.operation.node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class DeployResult extends OperationResult {

  private static final long serialVersionUID = 1L;

  private Map<String, Exception> _exceptionByShard = new HashMap<String, Exception>(3);

  public DeployResult(String nodeName) {
    super(nodeName);
  }

  public void addShardException(String shardName, Exception exception) {
    _exceptionByShard.put(shardName, exception);
  }

  public Set<Entry<String, Exception>> getShardExceptions() {
    return _exceptionByShard.entrySet();
  }

}
