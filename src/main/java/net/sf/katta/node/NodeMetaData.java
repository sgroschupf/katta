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
package net.sf.katta.node;

import java.io.Serializable;

import net.sf.katta.node.Node.NodeState;
import net.sf.katta.util.DefaultDateFormat;

import org.apache.hadoop.io.Text;

public class NodeMetaData implements Serializable {

  private Text _name = new Text();
  private NodeState _state;
  private float _queriesPerMinute = 0f;
  private long _startTimeStamp = System.currentTimeMillis();

  public NodeMetaData() {
    // for serialization
  }

  public NodeMetaData(final String name, NodeState nodeState) {
    _name = new Text(name);
    _state = nodeState;
  }

  public String getName() {
    return _name.toString();
  }

  public String getStartTimeAsDate() {
    return DefaultDateFormat.longToDateString(_startTimeStamp);
  }

  public NodeState getState() {
    return _state;
  }

  public void setState(NodeState state) {
    _state = state;
  }

  public float getQueriesPerMinute() {
    return _queriesPerMinute;
  }

  public void setQueriesPerMinute(final float queriesPerMinute) {
    _queriesPerMinute = queriesPerMinute;
  }

  @Override
  public String toString() {
    return getName() + "\t:\t" + getStartTimeAsDate() + "\t:\t" + getState();
  }

}
