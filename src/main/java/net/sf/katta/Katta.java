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
package net.sf.katta;

import java.util.List;

import net.sf.katta.client.Client;
import net.sf.katta.client.IClient;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.IPaths;
import net.sf.katta.slave.Hit;
import net.sf.katta.slave.Hits;
import net.sf.katta.slave.IQuery;
import net.sf.katta.slave.Query;
import net.sf.katta.slave.SlaveMetaData;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

public class Katta {

  public static void main(final String[] args) throws KattaException {
    if (args.length < 1) {
      usage();
    }
    final String command = args[0];
    final Katta katta = new Katta();
    if (command.equals("search")) {
      final String[] indexNames = args[1].split(",");
      final String query = args[2];
      if (args.length > 3) {
        final int count = Integer.parseInt(args[3]);
        katta.search(indexNames, query, count);
      } else {
        katta.search(indexNames, query);
      }
    } else if (command.endsWith("addIndex")) {
      addIndex(args[1], args[2], args[3]);
    } else if (command.endsWith("removeIndex")) {
      removeIndex(args[1]);
    } else if (command.endsWith("listIndexes")) {
      listIndex();
    } else if (command.endsWith("listSlaves")) {
      listSlaves();
    } else if (command.endsWith("startSlave")) {
      startSlave();
    } else if (command.endsWith("startMaster")) {
      startMaster();
    } else if (command.endsWith("showStructure")) {
      showStructure();
    }
  }

  private static void removeIndex(final String indexName) throws KattaException {
    final ZkConfiguration configuration = new ZkConfiguration();
    final ZKClient client = new ZKClient(configuration);
    client.waitForZooKeeper(5000);
    final String indexPath = IPaths.INDEXES + "/" + indexName;
    if (client.exists(indexPath)) {
      client.delete(indexPath);
    } else {
      System.err.println("Unknown index:" + indexName);
    }

  }

  private static void showStructure() throws KattaException {
    final ZKClient client = new ZKClient(new ZkConfiguration());
    client.waitForZooKeeper(5000);
    client.showFolders(System.out);
  }

  private static void startMaster() throws KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final Server server = new Server(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(5000);
    server.startMasterOrSlave(client, true);
    server.join();
  }

  private static void startSlave() throws KattaException {
    final ZkConfiguration configuration = new ZkConfiguration();
    final ZKClient client = new ZKClient(configuration);
    client.waitForZooKeeper(5000);
    final Server slave = new Server(configuration);
    slave.startMasterOrSlave(client, false);
    slave.join();
  }

  private static void listSlaves() throws KattaException {
    final ZKClient client = new ZKClient(new ZkConfiguration());
    client.waitForZooKeeper(5000);
    final List<String> slaves = client.getChildren(IPaths.SLAVES);
    if (null != slaves) {
      // header
      System.out.println("name \t:\t start time  \t:\t is healthy  \t:\t status");
      for (final String slave : slaves) {
        final String path = IPaths.SLAVES + "/" + slave;
        final SlaveMetaData slaveMetaData = new SlaveMetaData();
        client.readData(path, slaveMetaData);
        System.out.println(slaveMetaData.toString());

      }
    }
  }

  private static void listIndex() throws KattaException {
    final ZKClient client = new ZKClient(new ZkConfiguration());
    client.waitForZooKeeper(5000);
    final List<String> indexes = client.getChildren(IPaths.INDEXES);
    for (final String index : indexes) {
      final IndexMetaData indexMetaData = new IndexMetaData();
      client.readData(IPaths.INDEXES + "/" + index, indexMetaData);
      System.out.println("index: " + index + "\n\tisDeployed: " + indexMetaData.isDeployed() + "\n\tpath: "
          + indexMetaData.getPath());
      // maybe show shards
      // maybe show serving slaves..
      // maybe show replication level...
    }
  }

  private static void addIndex(final String name, final String path, final String analyzerClass) throws KattaException {
    final ZKClient client = new ZKClient(new ZkConfiguration());
    client.waitForZooKeeper(5000);
    client.showFolders(System.out);
    client.create(IPaths.INDEXES + "/" + name, new IndexMetaData(path, analyzerClass, false));
    // may be wait until the index is deployed...
    // maybe check if index is valid...
  }

  private void search(final String[] indexNames, final String queryString, final int count) throws KattaException {
    final IClient client = new Client();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final Hits hits = client.search(query, indexNames, count);
    final long end = System.currentTimeMillis();
    System.out.println(hits.size() + " hits found in " + ((end - start) / 1000.0) + "sec.");
    int index = 0;
    for (final Hit hit : hits.getHits()) {
      System.out.println("Hit: " + index);
      System.out.println("\tSlave:\t" + hit.getSlave());
      System.out.println("\tShard:\t" + hit.getShard());
      System.out.println("\tDocId:\t" + hit.getDocId());
      System.out.println("\tScore:\t" + hit.getScore());
      index++;
    }
  }

  private void search(final String[] indexNames, final String queryString) throws KattaException {
    final IClient client = new Client();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final int hitsSize = client.count(query, indexNames);
    final long end = System.currentTimeMillis();
    System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
  }

  private static void usage() {
    System.err.println("Usage: ");
    System.err
        .println("\tsearch <index name>[,<index name>,...] \"<query>\" [count]\tSearch in supplied indexes. The query should be in \". If you supply a result count hit details will be printed.");
    System.err.println("\tlistIndexes\tLists all indexes.");
    System.err.println("\tlistSlave\tLists all slave.");
    System.err.println("\tstartMaster\tStarts a local master.");
    System.err.println("\tstartSlave\tStarts a local slave.");
    System.err.println("\tshowStructure\tShows the structure of a Katta installation.");
    System.exit(1);
  }
}
