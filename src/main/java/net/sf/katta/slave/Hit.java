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
package net.sf.katta.slave;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 * 
 */
public class Hit implements Writable, Comparable<Hit> {

  /**
   * 
   */
  private static final long serialVersionUID = -4098882107088103222L;

  private Text _shard;

  private Text _slave;

  private float _score;

  private int _docId;

  public Hit(final String shard, final String slave, final float score, final int id) {
    _shard = new Text(shard);
    if (slave != null) {
      _slave = new Text(slave);
    } else {
      _slave = null;
    }
    _score = score;
    _docId = id;
  }

  public Hit() {
  }

  // public Hit(Text shardName, Text serverName, float score, int docId) {
  // _shard = shardName;
  // _slave = serverName;
  // _score = score;
  // _docId = docId;
  // }

  public String getShard() {
    return _shard.toString();
  }

  public String getSlave() {
    return _slave.toString();
  }

  public float getScore() {
    return _score;
  }

  public void readFields(final DataInput in) throws IOException {
    _score = in.readFloat();
    final boolean hasSlave = in.readBoolean();
    if (hasSlave) {
      _slave = new Text();
      _slave.readFields(in);
    } else {
      _slave = null;
    }
    _shard = new Text();
    _shard.readFields(in);
    _docId = in.readInt();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeFloat(_score);
    if (_slave != null) {
      out.writeBoolean(true);
      _slave.write(out);
    } else {
      out.writeBoolean(false);
    }
    _shard.write(out);
    out.writeInt(_docId);
  }

  public int compareTo(final Hit o) {
    int result = 1;
    if (_score > o.getScore()) {
      result = -1;
    }

    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    int temp;
    temp = Float.floatToIntBits(_score);
    result = prime * result + (temp ^ (temp >>> 32));
    result = prime * result + ((_slave == null) ? 0 : _slave.hashCode());
    result = prime * result + ((_shard == null) ? 0 : _shard.hashCode());
    result = prime * result + _docId;
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final Hit other = (Hit) obj;
    if (Float.floatToIntBits(_score) != Float.floatToIntBits(other._score))
      return false;
    if (_slave == null) {
      if (other._slave != null)
        return false;
    } else if (!_slave.equals(other._slave))
      return false;
    if (_shard == null) {
      if (other._shard != null)
        return false;
    } else if (!_shard.equals(other._shard))
      return false;
    if (_docId != other._docId)
      return false;
    return true;
  }

  public int getDocId() {
    return _docId;
  }

  public void setDocId(final int docId) {
    _docId = docId;
  }

  @Override
  public String toString() {
    return getSlave() + " " + getShard() + " " + getDocId();
  }
}
