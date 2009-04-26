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

import org.apache.hadoop.io.Writable;

public class ShardError implements Writable {

  private String _errorMsg;
  private long _timestamp = System.currentTimeMillis();

  public ShardError() {
    // for serialization
  }

  public ShardError(String errorMsg) {
    _errorMsg = errorMsg;
  }

  public String getErrorMsg() {
    return _errorMsg;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public void readFields(final DataInput in) throws IOException {
    _timestamp = in.readLong();
    _errorMsg = in.readUTF();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeLong(_timestamp);
    out.writeUTF(_errorMsg);
  }

  @Override
  public String toString() {
    return _timestamp + " " + _errorMsg;
  }
}
