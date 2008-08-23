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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IndexMetaData implements Writable {

  private Text _path;

  private Text _analyzerClassName;

  private int _replicationLevel;

  private IndexState _state;

  public enum IndexState {
    ANNOUNCED(0), DEPLOYED(1), DEPLOY_ERROR(2), UNDEPLOYED(3), NO_VALID_KATTA_INDEX(4), DEPLOYING(5), REBALANCING(6);
    private int _value;

    IndexState(final int value) {
      _value = value;
    }

    public int value() {
      return _value;
    }

    public static IndexState valueOf(final int value) {
      switch (value) {
      case 0:
        return ANNOUNCED;
      case 1:
        return DEPLOYED;
      case 2:
        return DEPLOY_ERROR;
      case 3:
        return UNDEPLOYED;
      case 5:
        return DEPLOYING;
      case 6:
        return REBALANCING;
      default:
        return DEPLOY_ERROR;
      }
    }
  }

  public IndexMetaData(final String path, final String analyzerName, final int replicationLevel, final IndexState state) {
    _path = new Text(path);
    _analyzerClassName = new Text(analyzerName);
    _replicationLevel = replicationLevel;
    _state = state;
  }

  public IndexMetaData() {
    // for serialization
  }

  public void readFields(final DataInput in) throws IOException {
    _path = new Text();
    _path.readFields(in);
    _analyzerClassName = new Text();
    _analyzerClassName.readFields(in);
    _replicationLevel = in.readInt();
    _state = IndexState.valueOf(in.readInt());

  }

  public void write(final DataOutput out) throws IOException {
    _path.write(out);
    _analyzerClassName.write(out);
    out.writeInt(_replicationLevel);
    out.writeInt(_state.value());

  }

  public String getPath() {
    return _path.toString();
  }

  public String getAnalyzerClassName() {
    return _analyzerClassName.toString();
  }

  public IndexState getState() {
    return _state;
  }

  public void setState(final IndexState state) {
    _state = state;
  }

  public int getReplicationLevel() {
    return _replicationLevel;
  }
}
