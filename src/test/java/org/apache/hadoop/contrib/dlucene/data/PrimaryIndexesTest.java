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
package org.apache.hadoop.contrib.dlucene.data;

import org.apache.hadoop.contrib.dlucene.IndexVersion;
import org.apache.hadoop.contrib.dlucene.TestUtils;

public class PrimaryIndexesTest extends TestUtils {

  private static String PIT_INDEX_TWO = getNextIndex();
  private static String L_INDEX_ONE = getNextIndex();
  
  private PrimaryIndexes indexes = null;
  private IndexVersion iv = null;
  
  protected void setUp() throws Exception {
    super.setUp();
    indexes = new PrimaryIndexes();
    iv = new IndexVersion(L_INDEX_ONE);
  }

  public void testAdd() {
    iv = iv.nextVersion();
    indexes.add(iv);
    assertEquals(iv, indexes.get(L_INDEX_ONE));
    indexes.add(iv);
    assertEquals(iv, indexes.get(L_INDEX_ONE));
    
    // make some more versions
    IndexVersion iv2 = iv.nextVersion();
    indexes.add(iv2);
    IndexVersion iv3 = iv2.nextVersion();
    indexes.add(iv3);
    assertEquals(iv3, indexes.get(L_INDEX_ONE));
  }
  
  public void testGetVersion() {
    try {
      indexes.get(UNKNOWN_INDEX);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      //
    }
  }
  
  public void testGetLocation() {
    try {
      indexes.get(UNKNOWN_INDEX);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      //
    }
  }

  public void testRemove() {
    IndexVersion iv4 = new IndexVersion(PIT_INDEX_TWO);
    indexes.add(iv4);
    assertEquals(iv4, indexes.get(PIT_INDEX_TWO));
    
    IndexVersion iv5 = iv4.nextVersion();
    indexes.add(iv5);
    assertEquals(iv5, indexes.get(PIT_INDEX_TWO));
    indexes.remove(iv5);
    assertEquals(iv4, indexes.get(PIT_INDEX_TWO));
    try {
      IndexVersion iv6 = new IndexVersion(UNKNOWN_INDEX);
      indexes.remove(iv6);
      fail("Should have thrown an exception");
    } catch (Exception e) {
      //
    }
  }
}
