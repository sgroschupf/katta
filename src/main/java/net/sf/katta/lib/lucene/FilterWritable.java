/**
 * Copyright 2011 the original author or authors.
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
package net.sf.katta.lib.lucene;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.Filter;

public class FilterWritable implements Writable {

  private Filter _filter;

  public FilterWritable() {
    // for serialization
  }

  public FilterWritable(Filter filter) {
    _filter = filter;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int readInt = input.readInt();
    byte[] bs = new byte[readInt];
    input.readFully(bs);
    ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(bs));
    try {
      _filter = (Filter) objectStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to deseriaize lucene filter", e);
    }

  }

  @Override
  public void write(DataOutput output) throws IOException {
    ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
    ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
    objectStream.writeObject(_filter);
    objectStream.close();
    byte[] byteArray = byteArrayStream.toByteArray();
    output.writeInt(byteArray.length);
    output.write(byteArray);
  }

  public Filter getFilter() {
    return _filter;
  }

  @Override
  public boolean equals(Object obj) {
    if (getClass() != obj.getClass()) {
      return false;
    }
    Filter other = ((FilterWritable) obj).getFilter();
    return _filter.equals(other);
  }

  @Override
  public String toString() {
    return _filter != null ? _filter.toString() : "null";
  }

}
