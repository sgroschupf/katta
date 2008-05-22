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

import org.apache.hadoop.ipc.VersionedProtocol;

/** Interface between client and namenode. */
public interface ClientToNameNodeProtocol extends VersionedProtocol {

  /** The version of the protocol. */
  long VERSION_ID = 1L;

  /**
   * Get the location of all indexes that can be searched.
   * 
   * @return All searchable indexes managed by this namenode.
   */
  IndexLocation[] getSearchableIndexes();

  /**
   * @return Get a new datanode at random. 
   */
  String getDataNode();
}
