/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.sf.katta.node.Node.NodeState;
import net.sf.katta.util.DefaultDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NodeMetaData implements Writable {

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

  public void readFields(final DataInput in) throws IOException {
    _name.readFields(in);
    _startTimeStamp = in.readLong();
    _state = NodeState.values()[in.readByte()];
    _queriesPerMinute = in.readFloat();
  }

  public void write(final DataOutput out) throws IOException {
    _name.write(out);
    out.writeLong(_startTimeStamp);
    out.writeByte(_state.ordinal());
    out.writeFloat(_queriesPerMinute);
  }

  @Override
  public String toString() {
    return getName() + "\t:\t" + getStartTimeAsDate() + "\t:\t" + getState();
  }

}
