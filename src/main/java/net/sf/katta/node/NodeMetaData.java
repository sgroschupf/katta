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

import net.sf.katta.util.DefaultDateFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NodeMetaData implements Writable {

  private Text _name = new Text();

  private Text _status = new Text();

  private boolean _isHealthy = true;

  private Text _exception = new Text();

  private float _queriesPerMinute = 0f;

  private long _startTimeStamp;

  private boolean _starting;

  public boolean isStarting() {
    return _starting;
  }

  public NodeMetaData() {
  }

  public NodeMetaData(final String name, final String status, final boolean isHealthy, final long startTime) {
    _name = new Text(name);
    _status = new Text(status);
    _isHealthy = isHealthy;
    _startTimeStamp = startTime;
  }

  public void setStatus(final String status) {
    _status = new Text(status);
  }

  public void setException(final String exception) {
    _exception = new Text(exception);
  }

  public void setQueriesPerMinute(final float queriesPerMinute) {
    _queriesPerMinute = queriesPerMinute;
  }

  public void readFields(final DataInput in) throws IOException {
    _name.readFields(in);
    _status.readFields(in);
    _queriesPerMinute = in.readFloat();
    _isHealthy = in.readBoolean();
    _exception.readFields(in);
    _startTimeStamp = in.readLong();
    _starting = in.readBoolean();

  }

  public void write(final DataOutput out) throws IOException {
    _name.write(out);
    _status.write(out);
    out.writeFloat(_queriesPerMinute);
    out.writeBoolean(_isHealthy);
    _exception.write(out);
    out.writeLong(_startTimeStamp);
    out.writeBoolean(_starting);
  }

  @Override
  public String toString() {
    String msg = getName() + "\t:\t" + getStartTimeAsDate() + "\t:\t" + isHealth() + "\t:\t" + getStatus();
    if (!isHealth()) {
      msg += "\n " + getException();
    }
    return msg;
  }

  private String getException() {
    return _exception.toString();
  }

  public String getStartTimeAsDate() {
    return DefaultDateFormat.longToDateString(_startTimeStamp);
  }

  public String getStatus() {
    return _status.toString();
  }

  public boolean isHealth() {
    return _isHealthy;
  }

  public String getName() {
    return _name.toString();
  }

  public void setStarting(final boolean starting) {
    _starting = starting;
  }

}
