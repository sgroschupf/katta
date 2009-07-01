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
package net.sf.katta.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class DeployedShard implements Writable {

  private String _shardName = "";
  private Map<String, String> _metaData = new HashMap<String, String>();

  public DeployedShard() {
    // for serialization
  }

  public DeployedShard(final String shardName, final Map<String, String> metaData) {
    _shardName = shardName != null ? shardName : "";
    _metaData = metaData != null ? metaData : new HashMap<String, String>();
  }

  public void readFields(final DataInput in) throws IOException {
    _shardName = in.readUTF();
    int keyCount = in.readInt();
    _metaData = new HashMap<String, String>();
    for (int i = 0; i < keyCount; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      _metaData.put(key, value);
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeUTF(_shardName);
    out.writeInt(_metaData.size());
    Iterator<String> iterator = _metaData.keySet().iterator();
    while (iterator.hasNext()) {
      String key = (String) iterator.next();
      String value = _metaData.get(key);
      if (value == null) {
        throw new IllegalArgumentException("null values in meta data not supported");
      }
      out.writeUTF(key);
      out.writeUTF(value);
    }
  }

  public String getShardName() {
    return _shardName;
  }

  public Map<String, String> getMetaData() {
    return _metaData;
  }

}
