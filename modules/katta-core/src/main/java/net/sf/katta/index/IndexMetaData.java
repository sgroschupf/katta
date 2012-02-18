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

@Deprecated
public class IndexMetaData implements Writable {

  private static final long serialVersionUID = 1L;

  private Text _path = new Text();
  private Text _analyzerClassName = new Text();
  private int _replicationLevel;
  private IndexState _state;
  private Text _errorMessage = new Text();

  @Deprecated
  public enum IndexState {
    ANNOUNCED, DEPLOYED, ERROR, DEPLOYING, REPLICATING;
  }

  public IndexMetaData(String path, String analyzerName, int replicationLevel, IndexState state) {
    this._path.set(path);
    this._analyzerClassName.set(analyzerName);
    this._replicationLevel = replicationLevel;
    this._state = state;
  }

  public IndexMetaData() {
    // for serialization
  }

  public String getPath() {
    return this._path.toString();
  }

  public String getAnalyzerClassName() {
    return this._analyzerClassName.toString();
  }

  public IndexState getState() {
    return this._state;
  }

  public void setState(IndexState state) {
    if (state == IndexState.ERROR) {
      throw new IllegalStateException("please set an error message");
    }
    this._state = state;
  }

  public void setState(IndexState state, String errorMsg) {
    this._state = state;
    if (errorMsg != null)
      this._errorMessage.set(errorMsg);
  }

  public String getErrorMessage() {
    return this._errorMessage.toString();
  }

  public int getReplicationLevel() {
    return this._replicationLevel;
  }

  public String toString() {
    return "state: " + _state + ", replication: " + _replicationLevel + ", path: " + _path + ", error: "
            + _errorMessage;
  }

  public void readFields(DataInput in) throws IOException {
    this._path.readFields(in);
    this._analyzerClassName.readFields(in);
    this._replicationLevel = in.readInt();
    this._state = IndexState.values()[in.readByte()];
    if (this._state == IndexState.ERROR)
      this._errorMessage.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    this._path.write(out);
    this._analyzerClassName.write(out);
    out.writeInt(this._replicationLevel);
    out.writeByte(this._state.ordinal());
    if (this._state == IndexState.ERROR)
      this._errorMessage.write(out);
  }
}
