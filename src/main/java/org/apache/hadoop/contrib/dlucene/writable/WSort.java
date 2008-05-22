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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * A {@link Writable} wrapper for {@link SortField}.
 */
public class WSort implements Writable {
  
  /** The Sort object being wrapped. */
  private Sort sort = null;

  /**
   * Constructor.
   * 
   * @param sort the Sort object
   */
  public WSort(Sort sort) {
    this.sort = sort;
  }

  /**
   * Constructor.
   */
  private WSort() {
    sort = new Sort();
  }

  /**
   * Get the Sort object being wrapped.
   * 
   * @return the Sort object
   */
  public Sort getSort() {
    return sort;
  }

  /**
   * Deserialize the Sort object.
   * 
   * @param in the input stream
   * @return the Sort object
   * @throws IOException
   */
  public static WSort read(DataInput in) throws IOException {
    WSort sort = new WSort();
    sort.readFields(in);
    return sort;
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
    out.writeInt(sort.getSort().length); // write values
    for (SortField s : sort.getSort()) {
      WSortField sortField = new WSortField(s);
      sortField.write(out);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    int n = in.readInt();
    SortField[] sf = new SortField[n];
    for (int i = 0; i < n; i++) {
      WSortField sortField = WSortField.read(in);
      sf[i] = sortField.getSortField();
    }
    sort.setSort(sf);
  }
}
