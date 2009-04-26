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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class IndexMetaData implements Writable {

  private Text _path = new Text();
  private Text _analyzerClassName = new Text();
  private int _replicationLevel;

  private IndexState _state;
  private Text _errorMessage = new Text();

  public enum IndexState {
    ANNOUNCED, DEPLOYED, ERROR, DEPLOYING, REPLICATING;
  }

  public IndexMetaData(final String path, final String analyzerName, final int replicationLevel, final IndexState state) {
    _path.set(path);
    _analyzerClassName.set(analyzerName);
    _replicationLevel = replicationLevel;
    _state = state;
  }

  public IndexMetaData() {
    // for serialization
  }

  public void readFields(final DataInput in) throws IOException {
    _path.readFields(in);
    _analyzerClassName.readFields(in);
    _replicationLevel = in.readInt();
    _state = IndexState.values()[in.readByte()];
    if (_state == IndexState.ERROR) {
      _errorMessage.readFields(in);
    }
  }

  public void write(final DataOutput out) throws IOException {
    _path.write(out);
    _analyzerClassName.write(out);
    out.writeInt(_replicationLevel);
    out.writeByte(_state.ordinal());
    if (_state == IndexState.ERROR) {
      _errorMessage.write(out);
    }
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
    if (state == IndexState.ERROR) {
      throw new IllegalStateException("please set an error message");
    }
    _state = state;
  }

  public void setState(final IndexState state, String errorMsg) {
    _state = state;
    if (errorMsg != null) {
      _errorMessage.set(errorMsg);
    }
  }

  public String getErrorMessage() {
    return _errorMessage.toString();
  }

  public int getReplicationLevel() {
    return _replicationLevel;
  }
  
  @Override
  public String toString() {
   return "state: "+_state + " replication: "+_replicationLevel + " path: "+_path + " error: "+_errorMessage;
  }
  
}
