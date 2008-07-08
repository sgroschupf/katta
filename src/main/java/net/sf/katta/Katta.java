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

import java.util.ArrayList;
import java.util.List;

import net.sf.katta.client.Client;
import net.sf.katta.client.IClient;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.IPaths;
import net.sf.katta.master.Master;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Query;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;

public class Katta {

  private final ZKClient _client;

  public Katta() {
    final ZkConfiguration configuration = new ZkConfiguration();
    _client = new ZKClient(configuration);
    _client.waitForZooKeeper(5000);
  }

  public static void main(final String[] args) throws KattaException {
    if (args.length < 1) {
      usage();
    }
    final String command = args[0];
    // static methods first
    if (command.endsWith("startNode")) {
      startNode();
    } else if (command.endsWith("startMaster")) {
      startMaster();
    } else {
      // non static methods
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
        int replication = 3;
        if (args.length == 5) {
          replication = Integer.parseInt(args[4]);
        }
        katta.addIndex(args[1], args[2], args[3], replication);
      } else if (command.endsWith("removeIndex")) {
        katta.removeIndex(args[1]);
      } else if (command.endsWith("listIndexes")) {
        katta.listIndex();
      } else if (command.endsWith("listNodes")) {
        katta.listNodes();
      } else if (command.endsWith("showStructure")) {
        katta.showStructure();
      }
    }
  }

  public static void startMaster() throws KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    client.waitForZooKeeper(30000);
    final Master master = new Master(client);
    master.start();
    zkServer.join();
  }

  public static void startNode() throws KattaException {
    final ZkConfiguration configuration = new ZkConfiguration();
    final ZKClient client = new ZKClient(configuration);
    client.waitForZooKeeper(30000);
    final Node node = new Node(client);
    node.start();
    node.join();
  }

  public void removeIndex(final String indexName) throws KattaException {
    final String indexPath = IPaths.INDEXES + "/" + indexName;
    if (_client.exists(indexPath)) {
      _client.deleteRecursiv(indexPath);
    } else {
      System.err.println("Unknown index:" + indexName);
    }

  }

  public void showStructure() throws KattaException {
    _client.showFolders();
  }

  public void listNodes() throws KattaException {
    final List<String> nodes = _client.getChildren(IPaths.NODES);
    if (null != nodes) {
      // header
      final Table table = new Table(new String[] { "Name", "Start time", "Healthy", "Status" });

      for (final String node : nodes) {
        final String path = IPaths.NODES + "/" + node;
        final NodeMetaData nodeMetaData = new NodeMetaData();
        _client.readData(path, nodeMetaData);
        table.addRow(new String[] { nodeMetaData.getName(), nodeMetaData.getStartTimeAsDate(),
            "" + nodeMetaData.isHealth(), nodeMetaData.getStatus() });
      }
      System.out.println(table.toString());
    }
  }

  public void listIndex() throws KattaException {
    final Table t = new Table(new String[] { "Name", "Deployed", "Analyzer", "Path" });

    final List<String> indexes = _client.getChildren(IPaths.INDEXES);
    for (final String index : indexes) {
      final IndexMetaData metaData = new IndexMetaData();
      _client.readData(IPaths.INDEXES + "/" + index, metaData);
      t.addRow(new String[] { index, metaData.getState().toString(), metaData.getAnalyzerClassName(),
          metaData.getPath() });
      // maybe show shards
      // maybe show serving nodes..
      // maybe show replication level...
    }
    System.out.println(t.toString());
  }

  public void addIndex(final String name, final String path, final String analyzerClass, final int replicationLevel)
      throws KattaException {
    final String indexPath = IPaths.INDEXES + "/" + name;
    if (!name.trim().equals("*")) {
      if (!_client.exists(indexPath)) {
        _client.create(indexPath, new IndexMetaData(path, analyzerClass, replicationLevel,
            IndexMetaData.IndexState.ANNOUNCED));
        final IndexMetaData data = new IndexMetaData();
        while (true) {
          _client.readData(indexPath, data);
          if (data.getState() == IndexMetaData.IndexState.DEPLOYED) {
            break;
          }
          System.out.print(".");
          try {
            Thread.sleep(1000);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
        System.out.println("deployed.");
      } else {
        System.out.println("Index with name " + name + " already exists.");
      }
    } else {
      System.out.println("Index with name " + name + " isn't allowed.");
    }
  }

  public void search(final String[] indexNames, final String queryString, final int count) throws KattaException {
    final IClient client = new Client();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final Hits hits = client.search(query, indexNames, count);
    final long end = System.currentTimeMillis();
    System.out.println(hits.size() + " hits found in " + ((end - start) / 1000.0) + "sec.");
    int index = 0;
    final Table table = new Table(new String[] { "Hit", "Node", "Shard", "DocId", "Score" });
    for (final Hit hit : hits.getHits()) {
      table
          .addRow(new String[] { "" + index, hit.getNode(), hit.getShard(), "" + hit.getDocId(), "" + hit.getScore() });
      index++;
    }
    System.out.println(table.toString());
  }

  public void search(final String[] indexNames, final String queryString) throws KattaException {
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
        .println("\tsearch <index name>[,<index name>,...] \"<query>\" [count]\tSearch in supplied indexes. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\"");
    System.err.println("\tlistIndexes\tLists all indexes.");
    System.err.println("\tlistNodes\tLists all nodes.");
    System.err.println("\tstartMaster\tStarts a local master.");
    System.err.println("\tstartNode\tStarts a local node.");
    System.err.println("\tshowStructure\tShows the structure of a Katta installation.");
    System.err.println("\tremoveIndex\tRemove a index from a Katta installation.");
    System.exit(1);
  }

  private class Table {
    private final String[] _header;
    private final List<String[]> _rows = new ArrayList<String[]>();

    public Table(final String[] header) {
      _header = header;
    }

    public void addRow(final String[] row) {
      _rows.add(row);
    }

    @Override
    public String toString() {
      final StringBuffer buffer = new StringBuffer();
      buffer.append("\n");
      final int[] columnSizes = getColumnSizes(_header, _rows);
      int rowWidth = 0;
      for (final int columnSize : columnSizes) {
        rowWidth += columnSize + 2;
      }
      // header
      buffer.append("| ");
      for (int i = 0; i < _header.length; i++) {
        final String column = _header[i];
        buffer.append(column + getChar(columnSizes[i] - column.length(), " ") + " | ");
      }
      buffer.append("\n=");
      buffer.append(getChar(rowWidth + columnSizes.length, "=") + "\n");

      for (final String[] row : _rows) {
        buffer.append("| ");
        for (int i = 0; i < row.length; i++) {
          buffer.append(row[i] + getChar(columnSizes[i] - row[i].length(), " ") + " | ");
        }
        buffer.append("\n-");
        buffer.append(getChar(rowWidth + columnSizes.length, "-") + "\n");
      }

      return buffer.toString();
    }

    private String getChar(final int count, final String character) {
      String spaces = "";
      for (int j = 0; j < count; j++) {
        spaces += character;
      }
      return spaces;
    }

    private int[] getColumnSizes(final String[] header, final List<String[]> rows) {
      final int[] sizes = new int[header.length];
      for (int i = 0; i < sizes.length; i++) {
        int min = header[i].length();
        for (final String[] row : rows) {
          if (row[i].length() > min) {
            min = row[i].length();
          }
        }
        sizes[i] = min;
      }

      return sizes;
    }
  }

  public void close() {
    if (_client != null) {
      _client.close();
    }
  }

}
