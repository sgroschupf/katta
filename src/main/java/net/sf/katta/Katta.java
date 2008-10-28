/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.client.Client;
import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.ShardError;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.index.indexer.merge.IndexMergeApplication;
import net.sf.katta.master.Master;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.Node;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.node.Query;
import net.sf.katta.node.Node.NodeState;
import net.sf.katta.tool.ZkTool;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.SymlinkResourceLoader;
import net.sf.katta.util.VersionInfo;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;
import net.sf.katta.zk.ZkServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides command line access to a Katta cluster.
 */
public class Katta {

  private final ZKClient _zkClient;

  public Katta() throws KattaException {
    final ZkConfiguration configuration = new ZkConfiguration();
    _zkClient = new ZKClient(configuration);
    _zkClient.start(10000);
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    final String command = args[0];
    // static methods first
    if (command.endsWith("startNode")) {
      startNode();
    } else if (command.endsWith("startMaster")) {
      startMaster();
    } else if (command.endsWith("version")) {
      showVersion();
    } else if (command.endsWith("zk")) {
      // TODO jz: cleanup the whole tool, command-line infrastruucture
      ZkTool.main(args);
    } else {
      // non static methods
      Katta katta = null;
      if (command.equals("search")) {
        final String[] indexNames = args[1].split(",");
        final String query = args[2];
        if (args.length > 3) {
          final int count = Integer.parseInt(args[3]);
          Katta.search(indexNames, query, count);
        } else {
          Katta.search(indexNames, query);
        }
      } else if (command.endsWith("addIndex")) {
        int replication = 3;
        if (args.length < 4) {
          printUsageAndExit();
        }
        if (args.length == 5) {
          replication = Integer.parseInt(args[4]);
        }
        katta = new Katta();
        katta.addIndex(args[1], args[2], args[3], replication);
      } else if (command.endsWith("removeIndex")) {
        katta = new Katta();
        katta.removeIndex(args[1]);
      } else if (command.endsWith("mergeIndexes")) {
        katta = new Katta();
        katta.mergeIndexes(args);
      } else if (command.endsWith("listIndexes")) {
        boolean detailedView = false;
        for (String arg : args) {
          if (arg.equals("-d")) {
            detailedView = true;
          }
        }
        katta = new Katta();
        katta.listIndex(detailedView);
      } else if (command.endsWith("listNodes")) {
        katta = new Katta();
        katta.listNodes();
      } else if (command.endsWith("showStructure")) {
        katta = new Katta();
        katta.showStructure();
      } else if (command.endsWith("check")) {
        katta = new Katta();
        katta.check();
      } else if (command.endsWith("listErrors")) {
        if (args.length > 1) {
          katta = new Katta();
          katta.showErrors(args[1]);
        } else {
          System.err.println("Missing parameter index name.");
          printUsageAndExit();
        }
      } else if (command.endsWith("redeployIndex")) {
        if (args.length > 1) {
          katta = new Katta();
          katta.redeployIndex(args[1]);
        } else {
          System.err.println("Missing parameter index name.");
          printUsageAndExit();
        }
      } else {
        System.err.println();
        System.err.println("> unknown command: '" + command + "'");
        System.err.println();
        printUsageAndExit();
      }
      if (katta != null) {
        katta.close();
      }
    }
  }

  private void redeployIndex(final String indexName) throws KattaException {
    String indexPath = ZkPathes.getIndexPath(indexName);
    if (!_zkClient.exists(indexPath)) {
      printError("index '" + indexName + "' does not exist");
      return;
    }

    IndexMetaData indexMetaData = new IndexMetaData();
    _zkClient.readData(indexPath, indexMetaData);
    try {
      removeIndex(indexName);
      Thread.sleep(5000);
      addIndex(indexName, indexMetaData.getPath(), indexMetaData.getAnalyzerClassName(), indexMetaData
          .getReplicationLevel());
    } catch (InterruptedException e) {
      printError("Redeployment of index '" + indexName + "' interrupted.");
    }

  }

  private void showErrors(final String indexName) throws KattaException {
    String indexZkPath = ZkPathes.getIndexPath(indexName);
    if (!_zkClient.exists(indexZkPath)) {
      printError("index '" + indexName + "' does not exist");
      return;
    }
    IndexMetaData indexMetaData = new IndexMetaData();
    _zkClient.readData(indexZkPath, indexMetaData);
    System.out.println("Error: " + indexMetaData.getErrorMessage());

    System.out.println("List of node-errors:");
    List<String> shards = _zkClient.getChildren(indexZkPath);
    for (String shardName : shards) {
      System.out.println("Shard: " + shardName);
      String shard2ErrorRootPath = ZkPathes.getShard2ErrorRootPath(shardName);
      if (_zkClient.exists(shard2ErrorRootPath)) {
        List<String> errors = _zkClient.getChildren(shard2ErrorRootPath);
        for (String nodeName : errors) {
          System.out.print("\tNode: " + nodeName);
          String shardToNodePath = ZkPathes.getShard2ErrorPath(shardName, nodeName);
          ShardError shardError = new ShardError();
          _zkClient.readData(shardToNodePath, shardError);
          System.out.println("\tError: " + shardError.getErrorMsg());
        }
      }
    }
  }

  private static void showVersion() {
    System.out.println("Katta '" + VersionInfo.VERSION + "'");
    System.out.println("Subversion '" + VersionInfo.SVN_URL + "'");
    System.out.println("Compiled by '" + VersionInfo.COMPILED_BY + "' on '" + VersionInfo.COMPILE_TIME + "'");
  }

  public static void startMaster() throws KattaException {
    final ZkConfiguration conf = new ZkConfiguration();
    final ZkServer zkServer = new ZkServer(conf);
    final ZKClient client = new ZKClient(conf);
    final Master master = new Master(client);
    master.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        master.shutdown();
      }
    });
    zkServer.join();
  }

  public static void startNode() throws KattaException, InterruptedException {
    final ZkConfiguration configuration = new ZkConfiguration();
    final ZKClient client = new ZKClient(configuration);
    final Node node = new Node(client);
    node.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        node.shutdown();
      }
    });
    node.join();
  }

  public void removeIndex(final String indexName) throws KattaException {
    IDeployClient deployClient = new DeployClient(_zkClient);
    if (!deployClient.existsIndex(indexName)) {
      printError("index '" + indexName + "' does not exist");
      return;
    }
    deployClient.removeIndex(indexName);
  }

  public void showStructure() throws KattaException {
    _zkClient.showFolders();
  }

  private void check() throws KattaException {
    // System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    System.out.println("Index Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    List<String> indexes = _zkClient.getChildren(ZkPathes.INDEXES);
    IndexMetaData indexMetaData = new IndexMetaData();
    CounterMap<IndexState> indexStateCounterMap = new CounterMap<IndexState>();
    for (String index : indexes) {
      _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
      indexStateCounterMap.increment(indexMetaData.getState());
    }
    Table tableIndexStates = new Table(new String[] { "index state", "count" });
    Set<IndexState> keySet = indexStateCounterMap.keySet();
    for (IndexState indexState : keySet) {
      tableIndexStates.addRow(indexState, indexStateCounterMap.getCount(indexState));
    }
    System.out.println(tableIndexStates.toString());
    System.out.println(indexes.size() + " indexes announced");

    System.out.println("\n");
    System.out.println("Shard Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    for (String index : indexes) {
      System.out.println("checking " + index + " ...");
      _zkClient.readData(ZkPathes.getIndexPath(index), indexMetaData);
      List<String> shards = _zkClient.getChildren(ZkPathes.getIndexPath(index));
      for (String shard : shards) {
        int shardReplication = _zkClient.countChildren(ZkPathes.getShard2NodeRootPath(shard));
        if (shardReplication < indexMetaData.getReplicationLevel()) {
          System.out.println("\tshard " + shard + " is under-replicated (" + shardReplication + "/"
              + indexMetaData.getReplicationLevel() + ")");
        } else if (shardReplication > indexMetaData.getReplicationLevel()) {
          System.out.println("\tshard " + shard + " is over-replicated (" + shardReplication + "/"
              + indexMetaData.getReplicationLevel() + ")");
        }
      }
    }

    System.out.println("\n");
    System.out.println("Node Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    Table tableNodeLoad = new Table("node", "connected", "deployed shards");
    List<String> nodes = _zkClient.getChildren(ZkPathes.NODE_TO_SHARD);
    for (String node : nodes) {
      boolean isConnected = _zkClient.exists(ZkPathes.getNodePath(node));
      int shardCount = _zkClient.countChildren(ZkPathes.getNode2ShardRootPath(node));
      StringBuilder builder = new StringBuilder();
      builder.append(" ");
      for (int i = 0; i < shardCount; i++) {
        builder.append("|");
      }
      builder.append(" ");
      builder.append(shardCount);
      tableNodeLoad.addRow(node, "" + isConnected, builder.toString());
    }
    System.out.println(tableNodeLoad);
  }

  public void listNodes() throws KattaException {
    final List<String> nodes = _zkClient.getKnownNodes();
    int inServiceNodeCount = 0;
    final Table table = new Table();
    for (final String node : nodes) {
      final String nodePath = ZkPathes.getNodePath(node);
      final NodeMetaData nodeMetaData = new NodeMetaData();
      if (_zkClient.exists(nodePath)) {
        _zkClient.readData(nodePath, nodeMetaData);
        NodeState nodeState = nodeMetaData.getState();
        if (nodeState == NodeState.IN_SERVICE) {
          inServiceNodeCount++;
        }
        table.addRow(nodeMetaData.getName(), nodeMetaData.getStartTimeAsDate(), nodeState.name());
      } else {
        // known but outdated node (master cleans this up)
      }
    }
    table.setHeader("Name (" + inServiceNodeCount + "/" + table.rowSize() + " nodes connected)", "Start time", "State");
    System.out.println(table.toString());
  }

  public void listIndex(boolean detailedView) throws KattaException, IOException {
    final Table table;
    if (!detailedView) {
      table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Documents", "Size" });
    } else {
      table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Documents", "Size", "Analyzer",
          "Replication" });
    }

    final List<String> indexes = _zkClient.getChildren(ZkPathes.INDEXES);
    for (final String index : indexes) {
      String indexZkPath = ZkPathes.getIndexPath(index);
      final IndexMetaData metaData = new IndexMetaData();
      _zkClient.readData(indexZkPath, metaData);

      String state = metaData.getState().toString();
      List<String> shards = _zkClient.getChildren(indexZkPath);
      int docCount = calculateDocCount(shards);
      long indexSize = calculateIndexSize(metaData.getPath());
      if (!detailedView) {
        table.addRow(index, state, metaData.getPath(), shards.size(), docCount, indexSize);
      } else {
        table.addRow(index, state, metaData.getPath(), shards.size(), docCount, indexSize, metaData
            .getAnalyzerClassName(), metaData.getReplicationLevel());
      }
    }
    if (table.rowSize() > 0) {
      System.out.println(table.toString());
    }
    System.out.println(indexes.size() + " registered indexes");
    System.out.println();
  }

  private long calculateIndexSize(String index) throws IOException {
    Path indexPath = new Path(index);
    URI indexUri = indexPath.toUri();
    FileSystem fileSystem = FileSystem.get(indexUri, new Configuration());
    if (!fileSystem.exists(indexPath)) {
      return 0;
    }
    return fileSystem.getContentSummary(indexPath).getLength();
  }

  private int calculateDocCount(List<String> shards) throws KattaException {
    int docCount = 0;
    for (String shard : shards) {
      List<String> deployedShards = _zkClient.getChildren(ZkPathes.getShard2NodeRootPath(shard));
      if (!deployedShards.isEmpty()) {
        DeployedShard deployedShard = new DeployedShard();
        _zkClient.readData(ZkPathes.getShard2NodePath(shard, deployedShards.get(0)), deployedShard);
        docCount += deployedShard.getNumOfDocs();
      }
    }
    return docCount;
  }

  public void addIndex(final String name, final String path, final String analyzerClass, final int replicationLevel)
      throws KattaException {
    final String indexZkPath = ZkPathes.getIndexPath(name);
    if (name.trim().equals("*")) {
      printError("Index with name " + name + " isn't allowed.");
      return;
    }
    if (_zkClient.exists(indexZkPath)) {
      printError("Index with name " + name + " already exists.");
      return;
    }

    try {
      IDeployClient deployClient = new DeployClient(_zkClient);
      IIndexDeployFuture deployFuture = deployClient.addIndex(name, path, analyzerClass, replicationLevel);
      while (true) {
        if (deployFuture.getState() == IndexState.DEPLOYED) {
          System.out.println("deployed index " + name);
          break;
        } else if (deployFuture.getState() == IndexMetaData.IndexState.ERROR) {
          System.err.println("not deployed index " + name);
          break;
        }
        System.out.print(".");
        deployFuture.joinDeployment(1000);
      }
    } catch (final InterruptedException e) {
      printError("interrupted wait on index deployment");
    }
  }

  private void mergeIndexes(String... args) throws Exception {
    String[] indexesToMerge = new String[0];
    File hadoopSiteXml = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-indexes")) {
        indexesToMerge = args[i + 1].split(",");
      } else if (args[i].equals("-hadoopSiteXml")) {
        hadoopSiteXml = new File(args[i + 1]);
      }
    }
    if (hadoopSiteXml != null) {
      if (!hadoopSiteXml.exists()) {
        throw new IllegalArgumentException("given hadoop-site.xml '" + hadoopSiteXml.getAbsolutePath()
            + "' does not exists");
      }
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      SymlinkResourceLoader classLoader = new SymlinkResourceLoader(contextClassLoader, "hadoop-site.xml",
          hadoopSiteXml);
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    IndexMergeApplication indexMergeApplication = new IndexMergeApplication(_zkClient);
    if (indexesToMerge.length == 0) {
      indexMergeApplication.mergeDeployedIndices();
    } else {
      indexMergeApplication.merge(indexesToMerge);
    }
  }

  public static void search(final String[] indexNames, final String queryString, final int count) throws KattaException {
    final IClient client = new Client();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final Hits hits = client.search(query, indexNames, count);
    final long end = System.currentTimeMillis();
    System.out.println(hits.size() + " hits found in " + ((end - start) / 1000.0) + "sec.");
    int index = 0;
    final Table table = new Table(new String[] { "Hit", "Node", "Shard", "DocId", "Score" });
    for (final Hit hit : hits.getHits()) {
      table.addRow(index, hit.getNode(), hit.getShard(), hit.getDocId(), hit.getScore());
      index++;
    }
    System.out.println(table.toString());
  }

  public static void search(final String[] indexNames, final String queryString) throws KattaException {
    final IClient client = new Client();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final int hitsSize = client.count(query, indexNames);
    final long end = System.currentTimeMillis();
    System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }

  private void printError(String errorMsg) {
    System.err.println("ERROR: " + errorMsg);
  }

  private static void printUsageAndExit() {
    System.err.println("Usage: ");
    System.err.println("\tlistIndexes [-d]\tLists all indexes. -d for detailed view.");
    System.err.println("\tlistNodes\t\tLists all nodes.");
    System.err.println("\tstartMaster\t\tStarts a local master.");
    System.err.println("\tstartNode\t\tStarts a local node.");
    System.err.println("\tshowStructure\t\tShows the structure of a Katta installation.");
    System.err.println("\tcheck\t\t\tAnalyze index/shard/node status.");
    System.err.println("\tversion\t\t\tPrint the version.");
    System.err
        .println("\taddIndex <index name> <path to index> <lucene analyzer class> [<replication level>]\tAdd a index to a Katta installation.");
    System.err.println("\tremoveIndex <index name>\tRemove a index from a Katta installation.");
    System.err.println("\tredeployIndex <index name>\tUndeploys and deploys an index.");
    System.err
        .println("\tmergeIndexes [-indexes <index1,index2>] [-hadoopSiteXml <siteXmlPath>]\tmergers all or the specified indexes.");
    System.err.println("\tlistErrors <index name>\t\tLists all deploy errors for a specified index.");
    System.err
        .println("\tsearch <index name>[,<index name>,...] \"<query>\" [count]\tSearch in supplied indexes. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\"");
    System.err.println();
    System.exit(1);
  }

  private static class Table {
    private String[] _header;
    private final List<Object[]> _rows = new ArrayList<Object[]>();

    public Table(final String... header) {
      _header = header;
    }

    public Table() {
      // setting header later
    }

    public void setHeader(String... header) {
      _header = header;
    }

    public void addRow(final Object... row) {
      _rows.add(row);
    }

    public int rowSize() {
      return _rows.size();
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append("\n");
      final int[] columnSizes = getColumnSizes(_header, _rows);
      int rowWidth = 0;
      for (final int columnSize : columnSizes) {
        rowWidth += columnSize + 2;
      }
      // header
      builder.append("| ");
      for (int i = 0; i < _header.length; i++) {
        final String column = _header[i];
        builder.append(column + getChar(columnSizes[i] - column.length(), " ") + " | ");
      }
      builder.append("\n=");
      builder.append(getChar(rowWidth + columnSizes.length, "=") + "\n");

      for (final Object[] row : _rows) {
        builder.append("| ");
        for (int i = 0; i < row.length; i++) {
          builder.append(row[i] + getChar(columnSizes[i] - row[i].toString().length(), " ") + " | ");
        }
        builder.append("\n-");
        builder.append(getChar(rowWidth + columnSizes.length, "-") + "\n");
      }

      return builder.toString();
    }

    private String getChar(final int count, final String character) {
      String spaces = "";
      for (int j = 0; j < count; j++) {
        spaces += character;
      }
      return spaces;
    }

    private int[] getColumnSizes(final String[] header, final List<Object[]> rows) {
      final int[] sizes = new int[header.length];
      for (int i = 0; i < sizes.length; i++) {
        int min = header[i].length();
        for (final Object[] row : rows) {
          int rowLength = row[i].toString().length();
          if (rowLength > min) {
            min = rowLength;
          }
        }
        sizes[i] = min;
      }

      return sizes;
    }
  }

  private static class CounterMap<K> {

    private Map<K, AtomicInteger> _counterMap = new HashMap<K, AtomicInteger>();

    public CounterMap() {
      super();
    }

    public void increment(K key) {
      AtomicInteger integer = _counterMap.get(key);
      if (integer == null) {
        integer = new AtomicInteger(0);
        _counterMap.put(key, integer);
      }
      integer.incrementAndGet();
    }

    public int getCount(K key) {
      AtomicInteger integer = _counterMap.get(key);
      if (integer == null) {
        return 0;
      }
      return integer.get();
    }

    public Set<K> keySet() {
      return _counterMap.keySet();
    }
  }

}
