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

package org.apache.hadoop.contrib.dlucene;

import junit.framework.TestCase;

import org.apache.lucene.document.Field;

public abstract class TestConstants extends TestCase {
  public static final String INDEX = "INDEX";
  public static final String NAMENODE = "namenode";
  public static final String[] RACKS = {"/rackA", "/rackB"};
  public static final String[] MACHINES = {"ringo", "george", "paul", "john", "stu"};
  public static final boolean USE_RAM_INDEX_FOR_TESTS = true;
  
  public static final String DATANODE_ADDRESS_ONE = MACHINES[0] + ":7001";
  public static final String DATANODE_ADDRESS_TWO = MACHINES[1] + ":7002";
  
  public static final String[] NAME = {"fred flintstone", "barney rubble", "gordon brown", "tony blair", "john smith", "michael howard", "boris johnson", "john prescott", "ken livingstone"};
  public static final String[] FORENAME = {"fred", "barney", "gordon", "tony", "john", "michael", "boris", "john"};
  public static final String[] SURNAME = {"flintstone", "rubble", "brown", "blair", "smith", "howard", "johnson", "prescott"};
  public static final String FIELD_KEY = "name";
  
  public static final Field exampleField = new Field("name", NAME[0], Field.Store.YES,
      Field.Index.TOKENIZED);
  
  public final static String UNKNOWN_INDEX = "someUnknownIndex";
  
  public final static String FIELD = "name";
}
