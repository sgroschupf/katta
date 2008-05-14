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
package net.sf.katta.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AssignedShard implements Writable {

  private String _indexName;

  private String _shardPath;

  public AssignedShard() {
  }

  public AssignedShard(final String indexName, final String shardPath) {
    _indexName = indexName;
    _shardPath = shardPath;
  }

  public void readFields(final DataInput in) throws IOException {
    _indexName = in.readUTF();
    _shardPath = in.readUTF();

  }

  public void write(final DataOutput out) throws IOException {
    out.writeUTF(_indexName);
    out.writeUTF(_shardPath);
  }

  public String getShardName() {
    return _indexName.toString() + "_" + getLastNodeName();
  }

  private String getLastNodeName() {
    final String shardPath = getShardPath();
    int lastIndexOf = shardPath.lastIndexOf("/");
    if (lastIndexOf == -1) {
      lastIndexOf = 0;
    }
    String name = shardPath.substring(lastIndexOf + 1, shardPath.length());
    if (name.endsWith(".zip")) {
      name = name.substring(0, name.length() - 4);
    }
    return name;
  }

  public String getShardPath() {
    return _shardPath.toString();
  }

  public String getIndexName() {
    return _indexName;
  }

  @Override
  public String toString() {
    return getShardName();
  }
}
