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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.client.IndexState;
import net.sf.katta.lib.lucene.Hit;
import net.sf.katta.lib.lucene.Hits;
import net.sf.katta.lib.lucene.ILuceneClient;
import net.sf.katta.lib.lucene.LuceneClient;
import net.sf.katta.master.Master;
import net.sf.katta.node.INodeManaged;
import net.sf.katta.node.Node;
import net.sf.katta.node.monitor.MetricLogger;
import net.sf.katta.node.monitor.MetricLogger.OutputType;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.ReplicationReport;
import net.sf.katta.protocol.metadata.IndexDeployError;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.tool.SampleIndexGenerator;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.util.StringUtil;
import net.sf.katta.util.VersionInfo;
import net.sf.katta.util.WebApp;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.util.ZkKattaUtil;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * Provides command line access to a Katta cluster.
 */
public class Katta {

  private final static List<Command> COMMANDS = new ArrayList<Command>();

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    Command command = null;
    try {
      command = getCommand(args[0]);
      command.validate(args);

      command.execute(args);
    } catch (Exception e) {
      printError(e.getMessage());
      if (command != null) {
        printUsageHeader();
        printUsage(command);
        printUsageFooter();
      }
      // e.printStackTrace();
      System.exit(1);
    }
  }

  protected static Command getCommand(String commandString) {
    for (Command command : COMMANDS) {
      if (commandString.equalsIgnoreCase(command.getCommand())) {
        return command;
      }
    }
    throw new IllegalArgumentException("no command for '" + commandString + "' found");
  }

  private static void printUsage(Command command) {
    System.err.println("  "
            + StringUtil.fillWithWhiteSpace(command.getCommand() + " " + command.getParameterString(), 60) + " "
            + command.getDescription());
  }

  private static void printUsageAndExit() {
    printUsageHeader();
    for (Command command : COMMANDS) {
      printUsage(command);
    }
    printUsageFooter();

    // TODO load test
    // System.err.println("\tstartLoadTestNode\tStarts a load test node.");
    // System.err
    // .println("\tstartLoadTest <nodes> <start-query-rate> <end-query-rate> <step> <test-duration-ms> <index-name> <query-file> <max hits>");
    // System.err.println("\t\t\t\tStarts a load test. The query rate is in queries per second.");

    System.exit(1);
  }

  private static void printUsageFooter() {
    System.err.println();
  }

  private static void printUsageHeader() {
    System.err.println("Usage: ");
  }

  protected static void addIndex(InteractionProtocol protocol, String name, String path, int replicationLevel) {
    IDeployClient deployClient = new DeployClient(protocol);
    if (name.trim().equals("*")) {
      throw new IllegalArgumentException("Index with name " + name + " isn't allowed.");
    }
    if (deployClient.existsIndex(name)) {
      throw new IllegalArgumentException("Index with name " + name + " already exists.");
    }

    try {
      long startTime = System.currentTimeMillis();
      IIndexDeployFuture deployFuture = deployClient.addIndex(name, path, replicationLevel);
      while (true) {
        long duration = System.currentTimeMillis() - startTime;
        if (deployFuture.getState() == IndexState.DEPLOYED) {
          System.out.println("\ndeployed index '" + name + "' in " + duration + " ms");
          break;
        } else if (deployFuture.getState() == IndexState.ERROR) {
          System.err.println("\nfailed to deploy index '" + name + "' in " + duration + " ms");
          break;
        }
        System.out.print(".");
        deployFuture.joinDeployment(1000);
      }
    } catch (final InterruptedException e) {
      printError("interrupted wait on index deployment");
    }
  }

  protected static void removeIndex(InteractionProtocol protocol, final String indexName) {
    IDeployClient deployClient = new DeployClient(protocol);
    if (!deployClient.existsIndex(indexName)) {
      throw new IllegalArgumentException("index '" + indexName + "' does not exist");
    }
    deployClient.removeIndex(indexName);
  }

  protected static void validateMinArguments(String[] args, int minCount) {
    if (args.length < minCount) {
      throw new IllegalArgumentException("not enough arguments");
    }
  }

  // public static void startIntegrationTest(int nodes, int startRate, int
  // endRate, int step, int runTime,
  // String[] indexNames, String queryFile, int count, ZkConfiguration conf) {
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
  // }

  // public static void startLoadTestNode(ZkConfiguration conf) {
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
  // }

  private static void printError(String errorMsg) {
    System.err.println("ERROR: " + errorMsg);
  }

  protected static Command START_MASTER_COMMAND = new Command("startMaster", "", "Starts a local master") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf) throws Exception {
      Master master = startMaster(zkConf);
      try {
        waitUntilJvmTerminates();
      } finally {
        master.shutdown();
      }
    }

    public Master startMaster(final ZkConfiguration conf) throws KattaException {
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

    protected void waitUntilJvmTerminates() {
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

  };

  protected static Command START_NODE_COMMAND = new Command("startNode", "[-c <serverClass>]", "Starts a local node") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf) throws Exception {
      NodeConfiguration nodeConfiguration = new NodeConfiguration();
      INodeManaged server = null;
      String serverClassName;
      if (optionMap.containsKey("-c")) {
        serverClassName = optionMap.get("-c");
      } else {
        serverClassName = nodeConfiguration.getServerClassName();
      }
      try {
        Class<?> serverClass = Katta.class.getClassLoader().loadClass(serverClassName);
        if (!INodeManaged.class.isAssignableFrom(serverClass)) {
          System.err.println("Class " + serverClassName + " does not implement " + INodeManaged.class.getName());
          System.exit(1);
        }
        server = (INodeManaged) serverClass.newInstance();
      } catch (ClassNotFoundException e) {
        System.err.println("Can not find class '" + serverClassName + "'");
        System.exit(1);
      } catch (InstantiationException e) {
        System.err.println("Could not create instance of class '" + serverClassName + "'");
        System.exit(1);
      } catch (IllegalAccessException e) {
        System.err.println("Unable to access class '" + serverClassName + "'");
        System.exit(1);
      } catch (Throwable t) {
        throw new RuntimeException("Error getting server instance for '" + serverClassName + "'", t);
      }
      final ZkClient client = ZkKattaUtil.startZkClient(zkConf, 60000);
      InteractionProtocol protocol = new InteractionProtocol(client, zkConf);
      final Node node = new Node(protocol, nodeConfiguration, server);
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

  };

  protected static Command LIST_NODES_COMMAND = new ProtocolCommand("listNodes", "", "Lists all nodes") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) {
      final List<String> knownNodes = protocol.getKnownNodes();
      final List<String> liveNodes = protocol.getLiveNodes();
      final Table table = new Table();
      int numNodes = 0;
      for (final String node : knownNodes) {
        numNodes++;
        NodeMetaData nodeMetaData = protocol.getNodeMD(node);
        table.addRow(nodeMetaData.getName(), nodeMetaData.getStartTimeAsDate(), liveNodes.contains(node) ? "CONNECTED"
                : "DISCONNECTED");
      }
      table.setHeader("Name (" + liveNodes.size() + "/" + knownNodes.size() + " connected)", "Start time", "State");
      System.out.println(table.toString());
    }
  };

  protected static Command LIST_INDICES_COMMAND = new ProtocolCommand("listIndices", "[-d]",
          "Lists all indexes. -d for detailed view.") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) {
      boolean detailedView = optionMap.containsKey("-d");
      final Table table;
      if (!detailedView) {
        table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Entries", "Disk Usage" });
      } else {
        table = new Table(new String[] { "Name", "Status", "Path", "Shards", "Entries", "Disk Usage", "Replication" });
      }

      List<String> indices = protocol.getIndices();
      for (final String index : indices) {
        final IndexMetaData indexMD = protocol.getIndexMD(index);
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
          table.addRow(index, state, indexMD.getPath(), shards.size(), entries, indexBytes, indexMD
                  .getReplicationLevel());
        }
      }
      if (!indices.isEmpty()) {
        System.out.println(table.toString());
      }
      System.out.println(indices.size() + " registered indexes");
      System.out.println();
    }

    private int calculateIndexEntries(Set<Shard> shards) {
      int docCount = 0;
      for (Shard shard : shards) {
        Map<String, String> metaData = shard.getMetaDataMap();
        if (metaData != null) {
          try {
            docCount += Integer.parseInt(metaData.get(INodeManaged.SHARD_SIZE_KEY));
          } catch (NumberFormatException e) {
            // ignore
          }
        }
      }
      return docCount;
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
  };

  protected static Command LOG_METRICS_COMMAND = new ProtocolCommand("logMetrics", "[sysout|log4j]",
          "Subscribes to the Metrics updates and logs them to log file or console") {

    public void validate(String[] args) {
      if (args.length < 2) {
        throw new IllegalArgumentException("no output type specified");
      }
      if (parseType(args[1]) == null) {
        throw new IllegalArgumentException("need to specify one of " + Arrays.asList(OutputType.values())
                + " as output type");
      }
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      OutputType outputType = parseType(args[1]);
      new MetricLogger(outputType, protocol).join();
    }

    private OutputType parseType(String typeString) {
      for (OutputType outputType : OutputType.values()) {
        if (typeString.equalsIgnoreCase(outputType.name())) {
          return outputType;
        }
      }
      return null;
    }

  };

  protected static Command START_GUI_COMMAND = new Command("startGui", "[-war <pathToWar>] [-port <port>]",
          "Starts the web based katta.gui") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf) throws Exception {
      int port = 8080;
      List<String> paths = new ArrayList<String>();
      paths.add(".");
      paths.add("./extras/katta.gui");

      if (args.length > 1) {
        for (int i = 1; i < args.length; i++) {
          String command = args[i];
          if (command.equals("-war")) {
            paths.clear();
            paths.add(args[i + 1]);
          } else if (command.equals("-port")) {
            port = Integer.parseInt(args[i + 1]);
          }
        }
      }
      WebApp app = new WebApp(paths.toArray(new String[paths.size()]), port);
      app.startWebServer();
    }
  };

  protected static Command SHOW_STRUCTURE_COMMAND = new ProtocolCommand("showStructure", "[-d]",
          "Shows the structure of a Katta installation. -d for detailed view.") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      boolean detailedView = optionMap.containsKey("-d");
      protocol.showStructure(detailedView);
    }
  };

  protected static Command CHECK_COMMAND = new ProtocolCommand("check", "", "Analyze index/shard/node status") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      System.out.println("            Index Analysis");
      System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
      List<String> indexes = protocol.getIndices();
      CounterMap<IndexState> indexStateCounterMap = new CounterMap<IndexState>();
      for (String index : indexes) {
        IndexMetaData indexMD = protocol.getIndexMD(index);
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
        IndexMetaData indexMD = protocol.getIndexMD(index);
        ReplicationReport replicationReport = protocol.getReplicationReport(indexMD);
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
      List<String> knownNodes = protocol.getKnownNodes();
      List<String> connectedNodes = protocol.getLiveNodes();
      for (String node : knownNodes) {
        boolean isConnected = connectedNodes.contains(node);
        int shardCount = 0;
        int announcedShardCount = 0;
        for (String shard : protocol.getNodeShards(node)) {
          shardCount++;
          long ctime = protocol.getShardAnnounceTime(node, shard);
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
      if (startTime < Long.MAX_VALUE && totalShards > 0 && totalAnnouncedShards > 0
              && totalAnnouncedShards < totalShards) {
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

  };

  // System.err.println("\tversion\t\t\tPrint the version.");

  protected static Command VERSION_COMMAND = new Command("version", "", "Print the version") {

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf) throws Exception {
      VersionInfo versionInfo = new VersionInfo();
      System.out.println("Katta '" + versionInfo.getVersion() + "'");
      System.out.println("Git-Revision '" + versionInfo.getRevision() + "'");
      System.out.println("Compiled by '" + versionInfo.getCompiledBy() + "' on '" + versionInfo.getCompileTime() + "'");
    }
  };

  protected static Command ADD_INDEX_COMMAND = new ProtocolCommand("addIndex",
          "<index name> <path to index> [<replication level>]", "Add a index to Katta") {

    public void validate(String[] args) {
      validateMinArguments(args, 3);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      String name = args[1];
      String path = args[2];
      int replicationLevel = 3;
      if (args.length == 4) {
        replicationLevel = Integer.parseInt(args[3]);
      }
      addIndex(protocol, name, path, replicationLevel);
    }

  };

  protected static Command REMOVE_INDEX_COMMAND = new ProtocolCommand("removeIndex", "<index name>",
          "Remove a index from Katta") {

    public void validate(String[] args) {
      validateMinArguments(args, 2);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      String indexName = args[1];
      removeIndex(protocol, indexName);
    }

  };

  protected static Command REDEPLOY_INDEX_COMMAND = new ProtocolCommand("redeployIndex", "<index name>",
          "Undeploys and deploys an index") {

    public void validate(String[] args) {
      validateMinArguments(args, 2);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      String indexName = args[1];
      IndexMetaData indexMD = protocol.getIndexMD(indexName);
      if (indexMD == null) {
        throw new IllegalArgumentException("index '" + indexName + "' does not exist");
      }
      removeIndex(protocol, indexName);
      Thread.sleep(5000);
      addIndex(protocol, indexName, indexMD.getPath(), indexMD.getReplicationLevel());
    }
  };

  protected static Command LIST_ERRORS_COMMAND = new ProtocolCommand("listErrors", "<index name>",
          "Lists all deploy errors for a specified index") {

    public void validate(String[] args) {
      validateMinArguments(args, 2);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      String indexName = args[1];
      IndexMetaData indexMD = protocol.getIndexMD(indexName);
      if (indexMD == null) {
        throw new IllegalArgumentException("index '" + indexName + "' does not exist");
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

  };

  protected static Command SEARCH_COMMAND = new ProtocolCommand(
          "search",
          "<index name>[,<index name>,...] \"<query>\" [count]",
          "Search in supplied indexes. The query should be in \". If you supply a result count hit details will be printed. To search in all indices write \"*\". This uses the client type LuceneClient.") {

    public void validate(String[] args) {
      validateMinArguments(args, 2);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      final String[] indexNames = args[1].split(",");
      final String query = args[2];
      if (args.length > 3) {
        final int count = Integer.parseInt(args[3]);
        search(indexNames, query, count);
      } else {
        search(indexNames, query);
      }
    }

    void search(final String[] indexNames, final String queryString, final int count) throws Exception {
      final ILuceneClient client = new LuceneClient();
      final Query query = new QueryParser(Version.LUCENE_CURRENT, "", new KeywordAnalyzer()).parse(queryString);
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

    void search(final String[] indexNames, final String queryString) throws Exception {
      final ILuceneClient client = new LuceneClient();
      final Query query = new QueryParser(Version.LUCENE_CURRENT, "", new KeywordAnalyzer()).parse(queryString);
      final long start = System.currentTimeMillis();
      final int hitsSize = client.count(query, indexNames);
      final long end = System.currentTimeMillis();
      System.out.println(hitsSize + " Hits found in " + ((end - start) / 1000.0) + "sec.");
    }

  };

  protected static Command GENERATE_INDEX_COMMAND = new ProtocolCommand("generateIndex",
          "<inputTextFile> <outputPath>  <numOfWordsPerDoc> <numOfDocuments>",
          "The inputTextFile is used as dictionary") {

    public void validate(String[] args) {
      validateMinArguments(args, 4);
    }

    @Override
    public void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception {
      String input = args[1];
      String output = args[2];
      int wordsPerDoc = Integer.parseInt(args[3]);
      int indexSize = Integer.parseInt(args[4]);

      SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
      sampleIndexGenerator.createIndex(input, output, wordsPerDoc, indexSize);
    }

  };

  static {
    COMMANDS.add(LIST_INDICES_COMMAND);
    COMMANDS.add(LIST_NODES_COMMAND);
    COMMANDS.add(START_MASTER_COMMAND);
    COMMANDS.add(START_NODE_COMMAND);
    // TODO load test xxx
    COMMANDS.add(LOG_METRICS_COMMAND);
    COMMANDS.add(START_GUI_COMMAND);
    COMMANDS.add(SHOW_STRUCTURE_COMMAND);
    COMMANDS.add(CHECK_COMMAND);
    COMMANDS.add(VERSION_COMMAND);
    COMMANDS.add(ADD_INDEX_COMMAND);
    COMMANDS.add(REMOVE_INDEX_COMMAND);
    COMMANDS.add(REDEPLOY_INDEX_COMMAND);
    COMMANDS.add(LIST_ERRORS_COMMAND);
    COMMANDS.add(GENERATE_INDEX_COMMAND);
    COMMANDS.add(SEARCH_COMMAND);

    Set<String> commandStrings = new HashSet<String>();
    for (Command command : COMMANDS) {
      if (!commandStrings.add(command.getCommand())) {
        throw new IllegalStateException("duplicated command sting " + command.getCommand());
      }
    }
  }

  static abstract class Command {

    private final String _command;
    private final String _parameterString;
    private final String _description;

    public Command(String command, String parameterString, String description) {
      _command = command;
      _parameterString = parameterString;
      _description = description;
    }

    @SuppressWarnings("unused")
    public void validate(String[] args) {
      // subclasses may override
    }

    public void execute(String[] args) throws Exception {
      execute(new ZkConfiguration(), args);
    }

    public void execute(ZkConfiguration zkConf, String[] args) throws Exception {
      execute(args, parseOptionMap(args), zkConf);
    }

    private Map<String, String> parseOptionMap(final String[] args) {
      Map<String, String> optionMap = new HashMap<String, String>();
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("-")) {
          String value = null;
          if (i < args.length - 1 && !args[i + 1].startsWith("-")) {
            value = args[i + 1];
          }
          optionMap.put(args[i], value);
        }
      }
      return optionMap;
    }

    protected abstract void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf)
            throws Exception;

    public String getCommand() {
      return _command;
    }

    public String getParameterString() {
      return _parameterString;
    }

    public String getDescription() {
      return _description;
    }
  }

  static abstract class ProtocolCommand extends Command {

    public ProtocolCommand(String command, String parameterString, String description) {
      super(command, parameterString, description);
    }

    @Override
    public final void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf) throws Exception {
      ZkClient zkClient = new ZkClient(zkConf.getZKServers());
      InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
      execute(args, optionMap, zkConf, protocol);
      protocol.disconnect();
    }

    protected abstract void execute(String[] args, Map<String, String> optionMap, ZkConfiguration zkConf,
            InteractionProtocol protocol) throws Exception;
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
