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

public class DeployedShard implements Writable {

  private String _shardName;
  private int _numOfDocs;
  private long _deployTime;

  public DeployedShard() {
    ;
  }

  public DeployedShard(final String shardName, final long currentTimeMillis, final int numOfDocs) {
    _shardName = shardName;
    _deployTime = currentTimeMillis;
    _numOfDocs = numOfDocs;
  }

  public void readFields(final DataInput in) throws IOException {
    _shardName = in.readUTF();
    _deployTime = in.readLong();
    _numOfDocs = in.readInt();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeUTF(_shardName);
    out.writeLong(_deployTime);
    out.writeInt(_numOfDocs);

  }

  public String getShardName() {
    return _shardName;
  }

  public long getDeployTime() {
    return _deployTime;
  }

  public int getNumOfDocs() {
    return _numOfDocs;
  }
}
