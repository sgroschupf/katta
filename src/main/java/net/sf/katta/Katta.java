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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.client.ILuceneClient;
import net.sf.katta.client.IndexState;
import net.sf.katta.client.LuceneClient;
import net.sf.katta.index.indexer.SampleIndexGenerator;
import net.sf.katta.master.Master;
import net.sf.katta.monitor.MetricLogger;
import net.sf.katta.monitor.MetricLogger.OutputType;
import net.sf.katta.node.Hit;
import net.sf.katta.node.Hits;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.IQuery;
import net.sf.katta.node.LuceneServer;
import net.sf.katta.node.Node;
import net.sf.katta.node.Query;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.ReplicationReport;
import net.sf.katta.protocol.metadata.DeployedShard;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.tool.ZkTool;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.VersionInfo;
import net.sf.katta.util.WebApp;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides command line access to a Katta cluster.
 */

// TODO sg: I guess we can make every method static here.
// TODO sg: review all exceptions
@SuppressWarnings("deprecation")
public class Katta {

  private final InteractionProtocol _protocol;

  public Katta() {
    this(new ZkConfiguration());
  }

  public Katta(final ZkConfiguration configuration) {
    this(configuration, ZkKattaUtil.startZkClient(configuration, 10000));
  }

  public Katta(final ZkConfiguration configuration, ZkClient zkClient) {
    _protocol = new InteractionProtocol(zkClient, configuration);
  }

  public InteractionProtocol getProtocol() {
    return _protocol;
  }

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    final ZkConfiguration configuration = new ZkConfiguration();

    final String command = args[0];
    // static methods first
    if (command.endsWith("startNode")) {
      startNode(args.length > 1 ? args[1] : null, configuration);
    } else if (command.endsWith("startMaster")) {
      runMaster(configuration);
    } else if (command.endsWith("startLoadTestNode")) {
      startLoadTestNode(configuration);
    } else if (command.endsWith("startLoadTest")) {
      int nodes = Integer.parseInt(args[1]);
      int startRate = Integer.parseInt(args[2]);
      int endRate = Integer.parseInt(args[3]);
      int step = Integer.parseInt(args[4]);
      final int runTime = Integer.parseInt(args[5]);
      final String[] indexNames = args[6].split(",");
      final String queryFile = args[7];
      final int count = Integer.parseInt(args[8]);
      startIntegrationTest(nodes, startRate, endRate, step, runTime, indexNames, queryFile, count, configuration);
    } else if (command.endsWith("startGui")) {
      startGui(args);
    } else if (command.endsWith("version")) {
      showVersion();
    } else if (command.endsWith("zk")) {
      // TODO jz: cleanup the whole tool, command-line infrastruucture
      ZkTool.main(args);
    } else if (command.endsWith("index")) {
      generateIndex(args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
    }

    else {
      // non static methods
      Katta katta = new Katta();
      // ZkClient zkClient = new ZkClient(configuration.getZKServers());

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
        if (args.length < 3) {
          printUsageAndExit();
        }
        if (args.length == 4) {
          replication = Integer.parseInt(args[3]);
        }
        katta.addIndex(args[1], args[2], replication);
      } else if (command.endsWith("removeIndex")) {
        katta.removeIndex(args[1]);
      } else if (command.endsWith("listIndexes") || command.endsWith("listIndices")) {
        boolean detailedView = false;
        for (String arg : args) {
          if (arg.equals("-d")) {
            detailedView = true;
          }
        }
        katta.listIndex(detailedView);
      } else if (command.endsWith("listNodes")) {
        katta.listNodes();
      } else if (command.endsWith("showStructure")) {
        boolean detailedView = false;
        for (String arg : args) {
          if (arg.equals("-d")) {
            detailedView = true;
          }
        }
        katta.showStructure(detailedView);
      } else if (command.endsWith("check")) {
        katta.check();
      } else if (command.endsWith("listErrors")) {
        if (args.length > 1) {
          katta.showErrors(args[1]);
        } else {
          System.err.println("Missing parameter index name.");
          printUsageAndExit();
        }
      } else if (command.endsWith("redeployIndex")) {
        if (args.length > 1) {
          katta.redeployIndex(args[1]);
        } else {
          System.err.println("Missing parameter index name.");
          printUsageAndExit();
        }
      } else if (command.endsWith("startMetricsLogger")) {
        OutputType type = OutputType.SystemOut;
        if (args.length > 1 && args[1].equalsIgnoreCase("Log4J")) {
          type = OutputType.Log4J;
        }
        new MetricLogger(type, katta.getProtocol()).join();

      } else {
        System.err.println();
        System.err.println("> unknown command: '" + command + "'");
        System.err.println();
        printUsageAndExit();
      }
      katta.close();
    }
  }

  private static void startGui(String[] args) throws Exception {
    int port = 8080;
    List<String> paths = new ArrayList<String>();
    paths.add(".");
    paths.add("./extras/katta.gui");

    if (args.length > 1) {
      for (int i = 1; i < args.length; i++) {
        String command = args[i];
        if (command.equals("-war")) {
          paths.add(0, args[i + 1]);
        } else if (command.equals("-port")) {
          port = Integer.parseInt(args[i + 1]);
        }
      }
    }
    WebApp app = new WebApp(paths.toArray(new String[paths.size()]), port);
    app.startWebServer();
  }

  public static void startIntegrationTest(int nodes, int startRate, int endRate, int step, int runTime,
          String[] indexNames, String queryFile, int count, ZkConfiguration conf) {
    // TODO: port Load Test over to new client/server setup.
    // final ZKClient client = new ZKClient(conf);
    // final LoadTestStarter integrationTester = new LoadTestStarter(client,
    // nodes, startRate, endRate, step, runTime, indexNames, queryFile, count);
    // integrationTester.start();
    // Runtime.getRuntime().addShutdownHook(new Thread() {
    // @Override
    // public void run() {
    // integrationTester.shutdown();
    // }
    // });
    // try {
    // while (client.isStarted()) {
    // Thread.sleep(100);
    // }
    // } catch (InterruptedException e) {
    // // terminate
    // }
  }

  public static void startLoadTestNode(ZkConfiguration conf) {
    // TODO: port load test to new client/server model.
    //
    // final ZKClient client = new ZKClient(conf);
    // final LoadTestNode testSearcher = new LoadTestNode(client, new
    // LoadTestNodeConfiguration());
    // testSearcher.start();
    // Runtime.getRuntime().addShutdownHook(new Thread() {
    // @Override
    // public void run() {
    // testSearcher.shutdown();
    // }
    // });
    // testSearcher.join();
  }

  private static void generateIndex(String input, String output, int wordsPerDoc, int indexSize) {
    SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
    sampleIndexGenerator.createIndex(input, output, wordsPerDoc, indexSize);
  }

  private void redeployIndex(final String indexName) {
    IndexMetaData indexMD = _protocol.getIndexMD(indexName);
    if (indexMD == null) {
      printError("index '" + indexName + "' does not exist");
      return;
    }
    try {
      removeIndex(indexName);
      Thread.sleep(5000);
      addIndex(indexName, indexMD.getPath(), indexMD.getReplicationLevel());
    } catch (InterruptedException e) {
      printError("Redeployment of index '" + indexName + "' interrupted.");
    }
  }

  public void showErrors(final String indexName) {
    IndexMetaData indexMD = _protocol.getIndexMD(indexName);
    if (indexMD == null) {
      printError("index '" + indexName + "' does not exist");
      return;
    }
    if (!indexMD.hasDeployError()) {
      System.out.println("No error for index '" + indexName + "'");
      return;
    }
    IndexDeployError deployError = indexMD.getDeployError();
    System.out.println("Error Type: " + deployError.getErrorType());
    if (deployError.getException() != null) {
      System.out.println("Error Message: " + deployError.getException().getMessage());
    }
    System.out.println("List of shard-errors:");
    Set<Shard> shards = indexMD.getShards();
    for (Shard shard : shards) {
      List<Exception> shardErrors = deployError.getShardErrors(shard.getName());
      if (shardErrors != null && !shardErrors.isEmpty()) {
        System.out.println("\t" + shard.getName() + ": ");
        for (Exception exception : shardErrors) {
          System.out.println("\t\t" + exception.getMessage());
        }
      }
    }
  }

  private static void showVersion() {
    VersionInfo versionInfo = new VersionInfo();
    System.out.println("Katta '" + versionInfo.getVersion() + "'");
    System.out.println("Git-Revision '" + versionInfo.getRevision() + "'");
    System.out.println("Compiled by '" + versionInfo.getCompiledBy() + "' on '" + versionInfo.getCompileTime() + "'");
  }

  public static void runMaster(final ZkConfiguration conf) throws KattaException {
    Master master = startMaster(conf);
    try {
      waitUntilJvmTerminates();
    } finally {
      master.shutdown();
    }
  }

  public static Master startMaster(final ZkConfiguration conf) throws KattaException {
    final Master master;
    if (conf.isEmbedded()) {
      ZkServer zkServer = ZkKattaUtil.startZkServer(conf);
      master = new Master(new InteractionProtocol(zkServer.getZkClient(), conf), zkServer);
    } else {
      ZkClient zkClient = ZkKattaUtil.startZkClient(conf, 30000);
      master = new Master(new InteractionProtocol(zkClient, conf), true);
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        master.shutdown();
      }
    });
    master.start();
    return master;
  }

  private static void waitUntilJvmTerminates() {
    // Just wait until the JVM terminates.
    Thread waiter = new Thread() {
      @Override
      public void run() {
        try {
          sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
          // terminate
        }
      }
    };
    waiter.setDaemon(true);
    waiter.start();
    try {
      waiter.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static void startNode(String serverClassName, final ZkConfiguration conf) throws InterruptedException {
    INodeManaged server = null;
    try {
      if (serverClassName == null) {
        serverClassName = LuceneServer.class.getName();
      }
      Class<?> serverClass = Katta.class.getClassLoader().loadClass(serverClassName);
      if (!INodeManaged.class.isAssignableFrom(serverClass)) {
        System.err.println("Class " + serverClassName + " does not implement INodeManaged!");
        System.exit(1);
      }
      server = (INodeManaged) serverClass.newInstance();
    } catch (ClassNotFoundException e) {
      System.err.println("Can not find class " + serverClassName + "!");
      System.exit(1);
    } catch (InstantiationException e) {
      System.err.println("Could not create instance of class " + serverClassName + "!");
      System.exit(1);
    } catch (IllegalAccessException e) {
      System.err.println("Unable to access class " + serverClassName + "!");
      System.exit(1);
    } catch (Throwable t) {
      throw new RuntimeException("Error getting server instance for " + serverClassName, t);
    }
    final ZkClient client = ZkKattaUtil.startZkClient(conf, 60000);
    InteractionProtocol interactionProtocol = new InteractionProtocol(client, conf);
    final Node node = new Node(interactionProtocol, server);
    node.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        node.shutdown();
        client.close();
      }
    });
    node.join();
  }

  // TODO sg: does this need to throw a katta exception?
  public void removeIndex(final String indexName) {
    IDeployClient deployClient = new DeployClient(_protocol);
    if (!deployClient.existsIndex(indexName)) {
      printError("index '" + indexName + "' does not exist");
      return;
    }
    deployClient.removeIndex(indexName);
  }

  // TODO sg: we might want to roll back in the -a flag.
  // public void showStructure(String arg) throws KattaException {
  // _zkClient.showFolders(arg != null && arg.startsWith("-a"), System.out);
  // }

  public void showStructure(boolean detailedView) {
    _protocol.showStructure(detailedView);
  }

  private void check() {
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    System.out.println("            Index Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    List<String> indexes = _protocol.getIndices();
    CounterMap<IndexState> indexStateCounterMap = new CounterMap<IndexState>();
    for (String index : indexes) {
      IndexMetaData indexMD = _protocol.getIndexMD(index);
      if (indexMD.hasDeployError()) {
        indexStateCounterMap.increment(IndexState.ERROR);
      } else {
        indexStateCounterMap.increment(IndexState.DEPLOYED);
      }
    }
    Table tableIndexStates = new Table("Index State", "Count");
    Set<IndexState> keySet = indexStateCounterMap.keySet();
    for (IndexState indexState : keySet) {
      tableIndexStates.addRow(indexState, indexStateCounterMap.getCount(indexState));
    }
    System.out.println(tableIndexStates.toString());
    System.out.println(indexes.size() + " indexes announced");

    System.out.println("\n");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    System.out.println("            Shard Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    for (String index : indexes) {
      System.out.println("checking " + index + " ...");
      IndexMetaData indexMD = _protocol.getIndexMD(index);
      ReplicationReport replicationReport = _protocol.getReplicationReport(indexMD);
      Set<Shard> shards = indexMD.getShards();
      for (Shard shard : shards) {
        int shardReplication = replicationReport.getReplicationCount(shard.getName());
        if (shardReplication < indexMD.getReplicationLevel()) {
          System.out.println("\tshard " + shard + " is under-replicated (" + shardReplication + "/"
                  + indexMD.getReplicationLevel() + ")");
        } else if (shardReplication > indexMD.getReplicationLevel()) {
          System.out.println("\tshard " + shard + " is over-replicated (" + shardReplication + "/"
                  + indexMD.getReplicationLevel() + ")");
        }
      }
    }

    System.out.println("\n");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    System.out.println("            Node Analysis");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    Table tableNodeLoad = new Table("Node", "Connected", "Shard Status");
    int totalShards = 0;
    int totalAnnouncedShards = 0;
    long startTime = Long.MAX_VALUE;
    List<String> knownNodes = _protocol.getKnownNodes();
    List<String> connectedNodes = _protocol.getLiveNodes();
    for (String node : knownNodes) {
      boolean isConnected = connectedNodes.contains(node);
      int shardCount = 0;
      int announcedShardCount = 0;
      for (String shard : _protocol.getNodeShards(node)) {
        shardCount++;
        long ctime = _protocol.getShardAnnounceTime(node, shard);
        if (ctime > 0) {
          announcedShardCount++;
          if (ctime < startTime) {
            startTime = ctime;
          }
        }
      }
      totalShards += shardCount;
      totalAnnouncedShards += announcedShardCount;
      StringBuilder builder = new StringBuilder();
      builder.append(String.format(" %9s ", String.format("%d/%d", announcedShardCount, shardCount)));
      for (int i = 0; i < shardCount; i++) {
        builder.append(i < announcedShardCount ? "#" : "-");
      }
      tableNodeLoad.addRow(node, Boolean.toString(isConnected), builder);
    }
    System.out.println(tableNodeLoad);
    double progress = totalShards == 0 ? 0.0 : (double) totalAnnouncedShards / (double) totalShards;
    System.out.printf("%d out of %d shards deployed (%.2f%%)\n", totalAnnouncedShards, totalShards, 100 * progress);
    if (startTime < Long.MAX_VALUE && totalShards > 0 && totalAnnouncedShards > 0 && totalAnnouncedShards < totalShards) {
      long elapsed = System.currentTimeMillis() - startTime;
      double timePerShard = (double) elapsed / (double) totalAnnouncedShards;
      long remaining = Math.round(timePerShard * (totalShards - totalAnnouncedShards));
      Date finished = new Date(System.currentTimeMillis() + remaining);
      remaining /= 1000;
      long secs = remaining % 60;
      remaining /= 60;
      long min = remaining % 60;
      remaining /= 60;
      System.out.printf("Estimated completion: %s (%dh %dm %ds)", finished, remaining, min, secs);
    }
  }

  public void listNodes() {
    final List<String> knownNodes = _protocol.getKnownNodes();
    final List<String> liveNodes = _protocol.getLiveNodes();
    final Table table = new Table();
    int numNodes = 0;
    for (final String node : knownNodes) {
      numNodes++;
      NodeMetaData nodeMetaData = _protocol.getNodeMD(node);
      table.addRow(nodeMetaData.getName(), nodeMetaData.getStartTimeAsDate(), liveNodes.contains(node) ? "CONNECTED"
              : "DISCONNECTED");
    }
    table.setHeader("Name (" + liveNodes.size() + "/" + knownNodes.size() + " connected)", "Start time", "State");
    System.out.println(table.toString());
  }

  public void listIndex(boolean detailedView) {
    final Table table;
    if (!detailedView) {
      table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Entries", "Disk Usage" });
    } else {
      table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Entries", "Disk Usage", "Replication" });
    }

    List<String> indices = _protocol.getIndices();
    for (final String index : indices) {
      final IndexMetaData indexMD = _protocol.getIndexMD(index);
      Set<Shard> shards = indexMD.getShards();
      String entries = "n/a";
      if (!indexMD.hasDeployError()) {
        entries = "" + calculateIndexEntries(shards);
      }
      long indexBytes = calculateIndexDiskUsage(indexMD.getPath());
      String state = indexMD.getDeployError() == null ? "DEPLOYED" : "ERROR";
      if (!detailedView) {
        table.addRow(index, state, indexMD.getPath(), shards.size(), entries, indexBytes);
      } else {
        table
                .addRow(index, state, indexMD.getPath(), shards.size(), entries, indexBytes, indexMD
                        .getReplicationLevel());
      }
    }
    if (!indices.isEmpty()) {
      System.out.println(table.toString());
    }
    System.out.println(indices.size() + " registered indexes");
    System.out.println();
  }

  private long calculateIndexDiskUsage(String index) {
    Path indexPath = new Path(index);
    URI indexUri = indexPath.toUri();
    try {
      FileSystem fileSystem = FileSystem.get(indexUri, new Configuration());
      if (!fileSystem.exists(indexPath)) {
        return -1;
      }
      return fileSystem.getContentSummary(indexPath).getLength();
    } catch (IOException e) {
      return -1;
    }
  }

  private int calculateIndexEntries(Set<Shard> shards) {
    int docCount = 0;
    for (Shard shard : shards) {
      List<DeployedShard> shardsMD = _protocol.getShardMDs(shard.getName());
      if (!shardsMD.isEmpty()) {
        Map<String, String> metaData = shardsMD.get(0).getMetaData();
        if (metaData != null) {
          try {
            docCount += Integer.parseInt(metaData.get(INodeManaged.SHARD_SIZE_KEY));
          } catch (NumberFormatException e) {
            // ignore
          }
        }
      }
    }
    return docCount;
  }

  public void addIndex(final String name, final String path, final int replicationLevel) {
    IDeployClient deployClient = new DeployClient(_protocol);
    if (name.trim().equals("*")) {
      printError("Index with name " + name + " isn't allowed.");
      return;
    }
    if (deployClient.existsIndex(name)) {
      printError("Index with name " + name + " already exists.");
      return;
    }

    try {
      IIndexDeployFuture deployFuture = deployClient.addIndex(name, path, replicationLevel);
      while (true) {
        if (deployFuture.getState() == IndexState.DEPLOYED) {
          System.out.println("deployed index " + name);
          break;
        } else if (deployFuture.getState() == IndexState.ERROR) {
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

  public static void search(final String[] indexNames, final String queryString, final int count) throws KattaException {
    final ILuceneClient client = new LuceneClient();
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
    final ILuceneClient client = new LuceneClient();
    final IQuery query = new Query(queryString);
    final long start = System.currentTimeMillis();
    final int hitsSize = client.count(query, indexNames);
    final long end = System.currentTimeMillis();
    System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
  }

  public void close() {
    if (_protocol != null) {
      _protocol.disconnect();
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
    System.err.println("\tstartNode [server classname]\t\tStarts a local node.");
    System.err.println("\tstartLoadTestNode\tStarts a load test node.");
    System.err
            .println("\tstartLoadTest <nodes> <start-query-rate> <end-query-rate> <step> <test-duration-ms> <index-name> <query-file> <max hits>");
    System.err.println("\t\t\t\tStarts a load test. The query rate is in queries per second.");

    System.err
            .println("\tstartMetricsLogger [sys|log4j]\t\tSubscribes to the Metrics updates and logs them to log file or console.");
    System.err.println("\tstartGui [-war <pathToWar>] [-port <port>]\t\tStarts the web based katta.gui.");

    System.err.println("\tshowStructure [-d]\t\tShows the structure of a Katta installation. -d for detailed view.");
    System.err.println("\tcheck\t\t\tAnalyze index/shard/node status.");
    System.err.println("\tversion\t\t\tPrint the version.");
    System.err
            .println("\taddIndex <index name> <path to index> [<replication level>]\tAdd a index to a Katta installation.");
    System.err.println("\tremoveIndex <index name>\tRemove a index from a Katta installation.");
    System.err.println("\tredeployIndex <index name>\tUndeploys and deploys an index.");
    System.err.println("\tlistErrors <index name>\t\tLists all deploy errors for a specified index.");
    System.err.println("\tsearch <index name>[,<index name>,...] \"<query>\" [count]\tSearch in supplied indexes. "
            + "The query should be in \". If you supply a result count hit details will be printed. "
            + "To search in all indices write \"*\". This uses the client type LuceneClient.");
    System.err
            .println("\tindex <inputTextFile> <outputPath>  <numOfWordsPerDoc> <numOfDocuments> \tGenerates a sample index. "
                    + "The inputTextFile is used as dictionary.");

    System.err.println();
    System.exit(1);
  }

  private static class Table {
    private String[] _header;
    private final List<String[]> _rows = new ArrayList<String[]>();

    public Table(final String... header) {
      _header = header;
    }

    /** Set the header later by calling setHeader() */
    public Table() {
      // default constructor
    }

    public void setHeader(String... header) {
      _header = header;
    }

    public void addRow(final Object... row) {
      String[] strs = new String[row.length];
      for (int i = 0; i < row.length; i++) {
        strs[i] = row[i] != null ? row[i].toString() : "";
      }
      _rows.add(strs);
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      final int[] columnSizes = getColumnSizes();
      int rowWidth = 0;
      for (final int columnSize : columnSizes) {
        rowWidth += columnSize;
      }
      rowWidth += 2 + (Math.max(0, columnSizes.length - 1) * 3) + 2;
      builder.append("\n" + getChar(rowWidth, "-") + "\n");
      // Header.
      builder.append("| ");
      String leftPad = "";
      for (int i = 0; i < _header.length; i++) {
        final String column = _header[i];
        builder.append(leftPad);
        builder.append(column + getChar(columnSizes[i] - column.length(), " "));
        leftPad = " | ";
      }
      builder.append(" |\n");
      builder.append(getChar(rowWidth, "=") + "\n");
      // Rows.
      for (final Object[] row : _rows) {
        builder.append("| ");
        leftPad = "";
        for (int i = 0; i < row.length; i++) {
          builder.append(leftPad);
          builder.append(row[i]);
          builder.append(getChar(columnSizes[i] - row[i].toString().length(), " "));
          leftPad = " | ";
        }
        builder.append(" |\n" + getChar(rowWidth, "-") + "\n");
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

    private int[] getColumnSizes() {
      final int[] sizes = new int[_header.length];
      for (int i = 0; i < sizes.length; i++) {
        int min = _header[i].length();
        for (final String[] row : _rows) {
          int rowLength = row[i].length();
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
