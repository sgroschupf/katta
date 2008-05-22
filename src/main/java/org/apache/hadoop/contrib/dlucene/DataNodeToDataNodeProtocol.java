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

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

/** Datanode to datanode protocol. */
public interface DataNodeToDataNodeProtocol extends VersionedProtocol {

  /** The version of the protocol. */
  long VERSION_ID = 1L;

  /**
   * Get the list of files used by this index. 
   * 
   * @param indexVersion The index version of the index.
   * @return A list of files used in that index.
   * @throws IOException
   */
  String[] getFileSet(IndexVersion indexVersion) throws IOException;

  /**
   * Get a particular file used in a Lucene index.
   * 
   * @param indexVersion The index.
   * @param file The file.
   * @return The file content.
   * @throws IOException
   */
  byte[] getFileContent(IndexVersion indexVersion, String file)
      throws IOException;
}
