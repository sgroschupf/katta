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
package net.sf.katta.slave;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TermWritable implements Writable {

  private String _text;

  private String _field;

  public TermWritable() {
    ;
  }

  public TermWritable(final String field, final String text) {
    _field = field;
    _text = text;
  }

  public void readFields(final DataInput in) throws IOException {
    _field = in.readUTF();
    _text = in.readUTF();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeUTF(_field);
    out.writeUTF(_text);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_field == null) ? 0 : _field.hashCode());
    result = prime * result + ((_text == null) ? 0 : _text.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final TermWritable other = (TermWritable) obj;

    if (_text == null) {
      if (other._text != null)
        return false;
    } else if (!_text.equals(other._text))
      return false;

    if (_field == null) {
      if (other._field != null)
        return false;
    } else if (!_field.equals(other._field))
      return false;

    return true;
  }

  @Override
  public String toString() {
    return _field + ":" + _text;
  }

}
