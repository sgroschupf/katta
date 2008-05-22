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
import org.apache.lucene.index.Term;

import org.apache.hadoop.io.Writable;

/**
 * A {@link Writable} wrapper for {@link Term}.
 */
public class WTerm implements Writable {
  
  /** The {@link Term} object being wrapped. */
  private Term term = null;

  /**
   * Constructor.
   * 
   * @param term the Term object
   */
  public WTerm(Term term) {
    this.term = term;
  }

  /**
   * Constructor.
   */
  private WTerm() {
    //
  }

  /**
   * Get the Term object.
   * 
   * @return the Term object
   */
  public Term getTerm() {
    return term;
  }

  /**
   * Deserialize the Term object.
   * 
   * @param in the input stream
   * @return the Term object
   * @throws IOException
   */
  public static WTerm read(DataInput in) throws IOException {
    WTerm term = new WTerm();
    term.readFields(in);
    return term;
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
    SafeText.writeString(out, term.field());
    SafeText.writeString(out, term.text());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException {
    Utils.checkArgs(in);
    String field = SafeText.readString(in);
    String text = SafeText.readString(in);
    term = new Term(field, text);
  }
}
