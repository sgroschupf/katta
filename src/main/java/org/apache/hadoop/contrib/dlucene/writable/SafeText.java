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

import org.apache.hadoop.io.Text;

/**
 * Some utilities for using {@link String} with 
 * {@link org.apache.hadoop.io.Writable}.
 */
public class SafeText {

  /**
   * Write a String to an output even if null.
   * 
   * @param out the output stream
   * @param string the string
   * @throws IOException
   */
  public static void writeString(DataOutput out, String string)
      throws IOException {
    Text.writeString(out, string == null ? "" : string);
  }

  /**
   * Read a String from an input even if null.
   * 
   * @param in the input stream
   * @return the string
   * @throws IOException
   */
  public static String readString(DataInput in) throws IOException {
    String s = Text.readString(in);
    return s.equals("") ? null : s;
  }
}
