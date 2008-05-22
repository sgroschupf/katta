/**
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
package org.apache.hadoop.contrib.dlucene.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.contrib.dlucene.Utils;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.SortField;

/**
 * A {@link Writable} wrapper for {@link SortField}.
 */
public class WSortField implements Writable {

  /** The SortField object being wrapped. */
  private SortField sf = null;

  /**
   * Constructor.
   * 
   * @param sortField the SortField object
   */
  public WSortField(SortField sortField) {
    this.sf = sortField;
  }

  /**
   * Constructor.
   */
  private WSortField() {
    sf = new SortField(null, SortField.SCORE);
  }

  /**
   * Get the SortField object being wrapped.
   * 
   * @return the Sortfield object
   */
  public SortField getSortField() {
    return sf;
  }

  /**
   * Deserialize the SortField object. 
   * 
   * @param in the input stream
   * @return the SortField object
   * @throws IOException
   */
  public static WSortField read(DataInput in) throws IOException {
    WSortField sf = new WSortField();
    sf.readFields(in);
    return sf;
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    Utils.checkArgs(out);
    String field = sf.getField();
    SafeText.writeString(out, field);
    out.writeBoolean(sf.getReverse());
    out.writeInt(sf.getType());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    String field = SafeText.readString(in);
    boolean reverse = in.readBoolean();
    int type = in.readInt();
    sf = new SortField(field, type, reverse);
  }
}
