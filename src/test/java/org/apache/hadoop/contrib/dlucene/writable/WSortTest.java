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

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.lucene.search.Sort;

public class WSortTest extends TestCase {

  private static Sort s = new Sort();
  private static WSort ws = new WSort(s);

  protected void setUp() throws Exception {
    super.setUp();
  }

  public void testGetSort() {
    assertEquals(ws.getSort(), s);
  }

  public void testReadAndWrite() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    ws.write(out);
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    WSort ret = WSort.read(in);
    assertEquals(s.toString(), ret.getSort().toString());
  }
}
