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
package net.sf.katta.node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class Query implements IQuery {

  private Text _query = new Text("");

  // private MapWritable _docFreqs = null;
  // private int _numDocs = 0;

  public Query() {
    // for serialization
  }

  public Query(final String query) {
    _query = new Text(query);
  }

  public void readFields(final DataInput in) throws IOException {
    _query = new Text();
    _query.readFields(in);
    // _numDocs = in.readInt();
    // if (in.readBoolean()) {
    // _docFreqs = new MapWritable();
    // _docFreqs.readFields(in);
    // } else {
    // _docFreqs = null;
    // }
  }

  public void write(final DataOutput out) throws IOException {
    _query.write(out);
    // out.writeInt(_numDocs);
    // if (_docFreqs != null) {
    // out.writeBoolean(true);
    // _docFreqs.write(out);
    // } else {
    // out.writeBoolean(false);
    // }
  }

  // public void setDocFreqs(MapWritable docFreqs) {
  // _docFreqs = docFreqs;
  // }
  //
  // public void setNumDocs(int numDocs) {
  // _numDocs = numDocs;
  // }

  public String getQuery() {
    return _query.toString();
  }

  @Override
  public String toString() {
    return getQuery();
  }

  //
  // public MapWritable getDocFreqs() {
  // return _docFreqs;
  // }
  //
  // public int getNumDocs() {
  // return _numDocs;
  // }

  @Override
  public int hashCode() {
    return _query.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return _query.equals(obj);
  }

}
