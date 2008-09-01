/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package net.sf.katta.master;

import net.sf.katta.zk.ZkPathes;

public interface IPaths {

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String ROOT_PATH = ZkPathes.ROOT_PATH;

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String MASTER = ZkPathes.MASTER;

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String NODES = ZkPathes.NODES;

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String INDEXES = ZkPathes.INDEXES;

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String NODE_TO_SHARD = ZkPathes.NODE_TO_SHARD;

  /**
   * @deprecated see {@link ZkPathes}
   */
  public static final String SHARD_TO_NODE = ZkPathes.SHARD_TO_NODE;

}
