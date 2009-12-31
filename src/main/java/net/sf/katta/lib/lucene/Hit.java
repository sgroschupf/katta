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
package net.sf.katta.lib.lucene;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.sf.katta.util.WritableType;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 * 
 */
public class Hit implements Writable, Comparable<Hit> {

  private static final long serialVersionUID = -4098882107088103222L;

  private Text _shard;

  private Text _node;

  private float _score;

  private int _docId;

  private WritableComparable[] _sortFields;

  private WritableType[] _sortFieldTypes;

  public Hit() {
    // needed for serialization
  }

  public Hit(final String shard, final String node, final float score, final int id) {
    this(shard, node, score, id, null);
  }

  /**
   * Construct a hit object with information about the types of the sort fields.
   */
  public Hit(final String shard, final String node, final float score, final int id, WritableType[] sortFieldTypes) {
    _shard = new Text(shard);
    if (node != null) {
      _node = new Text(node);
    } else {
      _node = null;
    }
    _score = score;
    _docId = id;
    _sortFieldTypes = sortFieldTypes;
  }

  // public Hit(Text shardName, Text serverName, float score, int docId) {
  // _shard = shardName;
  // _node = serverName;
  // _score = score;
  // _docId = docId;
  // }

  public String getShard() {
    return _shard.toString();
  }

  public String getNode() {
    return _node.toString();
  }

  public float getScore() {
    return _score;
  }

  public int getDocId() {
    return _docId;
  }

  public void setDocId(final int docId) {
    _docId = docId;
  }

  public void setSortFields(WritableComparable[] sortFields) {
    _sortFields = sortFields;
  }

  public WritableComparable[] getSortFields() {
    return _sortFields;
  }

  public void readFields(final DataInput in) throws IOException {
    _score = in.readFloat();
    final boolean hasNode = in.readBoolean();
    if (hasNode) {
      _node = new Text();
      _node.readFields(in);
    } else {
      _node = null;
    }
    _shard = new Text();
    _shard.readFields(in);
    _docId = in.readInt();
    byte sortFieldsLen = in.readByte();
    if (sortFieldsLen > 0) {
      _sortFields = new WritableComparable[sortFieldsLen];
      for (int i = 0; i < sortFieldsLen; i++) {
        _sortFields[i] = _sortFieldTypes[i].newWritableComparable();
        _sortFields[i].readFields(in);
      }
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeFloat(_score);
    if (_node != null) {
      out.writeBoolean(true);
      _node.write(out);
    } else {
      out.writeBoolean(false);
    }
    _shard.write(out);
    out.writeInt(_docId);
    if (_sortFields == null) {
      out.writeByte(0);
    } else {
      out.writeByte(_sortFields.length);
      for (Writable writable : _sortFields) {
        writable.write(out);
      }
    }
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
    result = prime * result + temp;
    result = prime * result + ((_node == null) ? 0 : _node.hashCode());
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
    if (_node == null) {
      if (other._node != null)
        return false;
    } else if (!_node.equals(other._node))
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

  @Override
  public String toString() {
    return getNode() + " " + getShard() + " " + getDocId() + " " + getScore();
  }
}
