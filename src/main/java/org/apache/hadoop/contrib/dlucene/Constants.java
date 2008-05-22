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

/**
 * Constants used in the DLucene package.
 */
public class Constants {

  /** The root directory for the Lucene filestore. */
  public static final String DEFAULT_ROOT_DIR = "tmp";

  public static final String VERSION_PREFIX = "version";

  public static final int BUFFER_SIZE = 16384; // TODO fixme - copied from BufferedIndexOutput

  public static final int MINIMUM_NUMBER_OF_REPLICAS = 3;

  public static final int MINIMUM_NUMBER_OF_RACKS = 2;

  public static final int MAXIMUM_NUMBER_OF_REPLICAS_PER_HEARTBEAT = 1;

  /** Character used to indicate this is a sharded index */
  public final static char SHARD_CHAR = '-';

  public static final String HEARTBEAT_INTERVAL_NAME = "dlucene.heartbeat.interval";
  
  public static final long HEARTBEAT_INTERVAL_VALUE = 3;
  
  public static final String LEASE_LENGTH_NAME = "dlucene.lease.length";
  
  public static final long LEASE_LENGTH_VALUE = 60;

  public static final String DEFAULT_VALUE = "default";

  public static final String HANDLER_COUNT_NAME = "dlucene.handler.count";

  public static final int HANDLER_COUNT_VALUE = 10;

  public static final String REPLICAS_NAME = "dlucene.replicas.number";

  public static final String REPLICAS_RACKS_NAME = "dlucene.replicas.racks";

  public static final String SATURATION_FACTOR_NAME = "dlucene.replicas.saturationfactor";
  public static final float SATURATION_FACTOR = (float) 0.75;

  public static final String MAXIMUM_NUMBER_OF_REPLICAS_PER_HEARTBEAT_NAME = "dlucene.replicas.heartbeat";

  public static final String HOST = "127.0.0.1";

  // namenode

  public static final int DLUCENE_NAMENODE_PORT = 7010;

  public static final String NAMENODE_DEFAULT_NAME = "dlucene.namenode.default.name";
  public static final String NAMENODE_DEFAULT_NAME_VALUE = HOST + ":"
      + Constants.DLUCENE_NAMENODE_PORT;

  public static final String NAMENODE_RACK_NAME = "dlucene.namenode.rack";

  // datanode

  public static final int DLUCENE_DATANODE_PORT = 7011;

  public static final String DATANODE_DEFAULT_NAME = "dlucene.datanode.default.name";
  public static final String DATANODE_DEFAULT_NAME_VALUE = HOST + ":"
      + Constants.DLUCENE_DATANODE_PORT;

  public static final String DNS_INTERFACE_NAME = "dlucene.datanode.dns.interface";
  public static final String DNS_NAMESERVER_NAME = "dlucene.datanode.dns.nameserver";

  public static final String DEFAULT_ROOT_DIR_NAME = "dlucene.data.dir";

  public static final String DATANODE_PORT_NAME = "dlucene.datanode.port";
  public static final String DATANODE_RACK_NAME = "dlucene.datanode.rack";
}
