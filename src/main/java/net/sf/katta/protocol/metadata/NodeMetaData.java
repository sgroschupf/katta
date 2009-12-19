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
package net.sf.katta.protocol.metadata;

import java.io.Serializable;

import net.sf.katta.util.DefaultDateFormat;

public class NodeMetaData implements Serializable {

  private static final long serialVersionUID = 1L;

  private String _name;
  private float _queriesPerMinute = 0f;
  private long _startTimeStamp = System.currentTimeMillis();

  // with node execution

  public NodeMetaData() {
    // for serialization
  }

  public NodeMetaData(final String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public String getStartTimeAsDate() {
    return DefaultDateFormat.longToDateString(_startTimeStamp);
  }

  public float getQueriesPerMinute() {
    return _queriesPerMinute;
  }

  public void setQueriesPerMinute(final float queriesPerMinute) {
    _queriesPerMinute = queriesPerMinute;
  }

  @Override
  public String toString() {
    return getName() + "\t:\t" + getStartTimeAsDate() + "";
  }

}
