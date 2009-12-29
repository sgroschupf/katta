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
package net.sf.katta.operation;

import java.io.Serializable;

public class OperationId implements Serializable {

  private static final long serialVersionUID = 1L;
  private final String _nodeName;
  private final String _elementName;

  public OperationId(String nodeName, String elementName) {
    _nodeName = nodeName;
    _elementName = elementName;
  }

  public String getNodeName() {
    return _nodeName;
  }

  public String getElementName() {
    return _elementName;
  }

  @Override
  public String toString() {
    return _nodeName + "-" + _elementName;
  }

}
