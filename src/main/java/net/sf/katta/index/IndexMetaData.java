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

public class IndexMetaData implements Serializable {

  private static final long serialVersionUID = 1L;
  
  private String _path;
  private int _replicationLevel;

  private IndexState _state;
  private String _errorMessage = "";

  public enum IndexState {
    ANNOUNCED, DEPLOYED, ERROR, DEPLOYING, REPLICATING;
  }

  public IndexMetaData(String path, int replicationLevel, IndexState state) {
    _path = path;
    _replicationLevel = replicationLevel;
    _state = state;
  }

  public IndexMetaData() {
    // for serialization
  }

  public String getPath() {
    return _path;
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

  public void setState(final IndexState state, String errorMessage) {
    _state = state;
    if (errorMessage != null) {
      _errorMessage = errorMessage;
    }
  }

  public String getErrorMessage() {
    return _errorMessage;
  }

  public int getReplicationLevel() {
    return _replicationLevel;
  }

  @Override
  public String toString() {
    return "state: " + _state + " replication: " + _replicationLevel + " path: " + _path + " error: " + _errorMessage;
  }

}
