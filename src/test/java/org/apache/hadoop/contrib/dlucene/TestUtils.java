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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

public abstract class TestUtils extends TestConstants {

  public static final Configuration conf = new Configuration();

  private static int nextPort = 7010;
  private static int nextIndex = 1;

  public static String getNextIndex() {
    String result = TestConstants.INDEX + nextIndex;
    nextIndex++;
    return result;
  }

  public static Configuration getConfiguration() {
    conf.setLong("dlucene.heartbeat.interval", 4);
    conf.setInt("dlucene.replicas.number", 2);
    conf.setInt("dlucene.replicas.racks", 1);
    conf.setInt("dlucene.replicas.heartbeat", 1);
    return conf;
  }

  public static int getNextPort() {
    if (nextPort == 7010) {
      TestUtils.deleteDirectory(new File(Constants.DEFAULT_ROOT_DIR));
    }
    return nextPort++;
  }

  public static Set<IndexLocation> toSet(HeartbeatResponse hbr) {
    if (hbr == null) { 
      return null;
    }
    IndexLocation[] il = hbr.getReplicationRequests();
    return toSet(il);
  }
  
  public static Set<IndexLocation> toSet(IndexLocation[] il) {
    if (il == null) {
      return null;
    }
    Set<IndexLocation> result = new HashSet<IndexLocation>();
    for (IndexLocation i : il) {
      result.add(i);
    }
    return result;
  }

  public static Document makeDocument(String fieldName, String fieldValue) {
    Document doc = new Document();
    doc.add(new Field(fieldName, fieldValue, Field.Store.YES,
        Field.Index.TOKENIZED));
    return doc;
  }

  public static Query getQuery(String field, String value)
      throws ParseException {
    return new QueryParser(field, new StandardAnalyzer()).parse(value);
  }

  public static boolean deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      for (String child : dir.list()) {
        if (!deleteDirectory(new File(dir + File.separator + child))) {
          return false;
        }
      }
    }
    // The directory is now empty so delete it
    return dir.delete();
  }

  public static DataNodeStatus makeDataNodeStatus(String machine, String rack)
      throws Exception {
    // create a single datanode
    InetSocketAddress addr = new InetSocketAddress(machine, TestUtils
        .getNextPort());
    DataNodeConfiguration dnc = new DataNodeConfiguration(addr, rack, Constants.DEFAULT_ROOT_DIR);
    DataNodeStatus status = new DataNodeStatus(dnc, conf);
    status.updateUsage();
    return status;
  }
}
