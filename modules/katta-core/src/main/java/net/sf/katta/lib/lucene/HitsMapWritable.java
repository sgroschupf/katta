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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.util.WritableType;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class HitsMapWritable implements Writable {

  private final static Logger LOG = Logger.getLogger(HitsMapWritable.class);

  private String _nodeName;
  private int _totalHits;
  private WritableType[] _sortFieldTypes;

  private List<Hit> _hits;
  private Set<String> _shards;

  public HitsMapWritable() {
    // for serialization
  }

  public HitsMapWritable(final String nodeName) {
    _nodeName = nodeName;
    _hits = new ArrayList<Hit>();
    _shards = new HashSet<String>();
  }

  public void readFields(final DataInput in) throws IOException {
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    _nodeName = in.readUTF();
    _totalHits = in.readInt();
    byte sortFieldTypesLen = in.readByte();
    if (sortFieldTypesLen > 0) {
      _sortFieldTypes = new WritableType[sortFieldTypesLen];
      for (int i = 0; i < sortFieldTypesLen; i++) {
        _sortFieldTypes[i] = WritableType.values()[in.readByte()];
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("HitsMap reading start at: " + start + " for server " + _nodeName);
    }
    final int shardCount = in.readInt();
    HashMap<Byte, String> shardByShardIndex = new HashMap<Byte, String>(shardCount);
    _shards = new HashSet<String>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      String shardName = in.readUTF();
      shardByShardIndex.put((byte) i, shardName);
      _shards.add(shardName);
    }

    final int hitCount = in.readInt();
    _hits = new ArrayList<Hit>(hitCount + 1);
    for (int i = 0; i < hitCount; i++) {
      final byte shardIndex = in.readByte();
      final float score = in.readFloat();
      final int docId = in.readInt();
      final String shard = shardByShardIndex.get(shardIndex);
      final Hit hit;
      if (sortFieldTypesLen > 0) {
        hit = new Hit(shard, _nodeName, score, docId, _sortFieldTypes);
      } else {
        hit = new Hit(shard, _nodeName, score, docId);
      }
      addHit(hit);
      byte sortFieldsLen = in.readByte();
      if (sortFieldsLen > 0) {
        WritableComparable[] sortFields = new WritableComparable[sortFieldsLen];
        for (int k = 0; k < sortFieldsLen; k++) {
          sortFields[k] = _sortFieldTypes[k].newWritableComparable();
          sortFields[k].readFields(in);
        }
        hit.setSortFields(sortFields);
      }
    }

    if (LOG.isDebugEnabled()) {
      final long end = System.currentTimeMillis();
      LOG.debug("HitsMap reading of " + hitCount + " entries took " + (end - start) / 1000.0 + "sec.");
    }
  }

  public void write(final DataOutput out) throws IOException {
    long start = 0;
    if (LOG.isDebugEnabled()) {
      start = System.currentTimeMillis();
    }
    out.writeUTF(_nodeName);
    out.writeInt(_totalHits);
    if (_sortFieldTypes == null) {
      out.writeByte(0);
    } else {
      out.writeByte(_sortFieldTypes.length);
      for (WritableType writableType : _sortFieldTypes) {
        out.writeByte(writableType.ordinal());
      }
    }
    int shardCount = _shards.size();
    out.writeInt(shardCount);
    byte shardIndex = 0;
    Map<String, Byte> shardIndexByShard = new HashMap<String, Byte>(shardCount);
    for (String shard : _shards) {
      out.writeUTF(shard);
      shardIndexByShard.put(shard, shardIndex);
      shardIndex++;
    }
    out.writeInt(_hits.size());
    for (Hit hit : _hits) {
      out.writeByte(shardIndexByShard.get(hit.getShard()));
      out.writeFloat(hit.getScore());
      out.writeInt(hit.getDocId());
      WritableComparable[] sortFields = hit.getSortFields();
      if (sortFields == null) {
        out.writeByte(0);
      } else {
        out.writeByte(sortFields.length);
        for (Writable writable : sortFields) {
          writable.write(out);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      final long end = System.currentTimeMillis();
      LOG.debug("HitsMap writing took " + (end - start) / 1000.0 + "sec.");
      LOG.debug("HitsMap writing ended at: " + end + " for server " + _nodeName);
    }
  }

  public void addHit(final Hit hit) {
    _hits.add(hit);
    _shards.add(hit.getShard());
  }

  /**
   * @deprecated use {@link #addHit(Hit)} instead
   */
  public void addHitToShard(final String shard, final Hit hit) {
    addHit(hit);
  }

  /**
   * @deprecated use {@link #getNodeName()} instead
   */
  public String getServerName() {
    return getNodeName();
  }

  public String getNodeName() {
    return _nodeName;
  }

  public List<Hit> getHitList() {
    return _hits;
  }

  /**
   * @deprecated use {@link #getHitList()} instead
   */
  public Hits getHits() {
    final Hits result = new Hits();
    result.setTotalHits(_totalHits);
    result.addHits(_hits);
    return result;
  }

  public void addTotalHits(final int length) {
    _totalHits += length;
  }

  public int getTotalHits() {
    return _totalHits;
  }

  public WritableType[] getSortFieldTypes() {
    return _sortFieldTypes;
  }

  public void setSortFieldTypes(WritableType[] sortFieldTypes) {
    _sortFieldTypes = sortFieldTypes;
  }

}
