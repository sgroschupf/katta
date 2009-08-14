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

import java.io.Serializable;

import org.apache.hadoop.io.Text;

public class IndexMetaData implements Serializable {

  private Text _path = new Text();
  private int _replicationLevel;

  private IndexState _state;
  private Text _errorMessage = new Text();

  public enum IndexState {
    ANNOUNCED, DEPLOYED, ERROR, DEPLOYING, REPLICATING;
  }

  public IndexMetaData(final String path, final int replicationLevel, final IndexState state) {
    _path.set(path);
    _replicationLevel = replicationLevel;
    _state = state;
  }

  public IndexMetaData() {
    // for serialization
  }

  public String getPath() {
    return _path.toString();
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
    return "state: " + _state + " replication: " + _replicationLevel + " path: " + _path + " error: " + _errorMessage;
  }

}
