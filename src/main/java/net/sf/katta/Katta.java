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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
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
import net.sf.katta.tool.loadtest.LoadTestMasterOperation;
import net.sf.katta.tool.loadtest.query.AbstractQueryExecutor;
import net.sf.katta.tool.loadtest.query.LuceneSearchExecutor;
import net.sf.katta.tool.loadtest.query.MapfileAccessExecutor;
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
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * Provides command line access to a Katta cluster.
 */
public class Katta {

  protected static final Logger LOG = Logger.getLogger(Katta.class);
  private final static List<Command> COMMANDS = new ArrayList<Command>();

  public static void main(final String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    Command command = null;
    try {
      command = getCommand(args[0]);
      command.parseArguments(args);
      command.execute();
    } catch (Exception e) {
      printError(e.getMessage());
      if (parseOptionMap(args).containsKey("-s")) {
        e.printStackTrace();
      }
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
    System.exit(1);
  }

  private static void printUsageFooter() {
    System.err.println();
  }

  private static void printUsageHeader() {
    System.err.println("Usage: ");
  }

  protected static Map<String, String> parseOptionMap(final String[] args) {
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

  protected static <T> Class<T> loadClass(String serverClassName, Class<T> instanceOfClass) {
    try {
      Class<?> loadedClass = Katta.class.getClassLoader().loadClass(serverClassName);
      if (!instanceOfClass.isAssignableFrom(loadedClass)) {
        throw new IllegalStateException("Class " + serverClassName + " does not implement " + instanceOfClass.getName());
      }
      return (Class<T>) loadedClass;
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Can not find class '" + serverClassName + "'");
    }
  }

  protected static void validateMinArguments(String[] args, int minCount) {
    if (args.length < minCount) {
      throw new IllegalArgumentException("not enough arguments");
    }
  }

  private static void printError(String errorMsg) {
    System.err.println("ERROR: " + errorMsg);
  }

  protected static Command START_ZK_COMMAND = new Command("startZk", "", "Starts a local zookeeper server") {

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
      final ZkServer zkServer = ZkKattaUtil.startZkServer(zkConf);
      synchronized (zkServer) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            synchronized (zkServer) {
              System.out.println("stopping zookeeper server...");
              zkServer.shutdown();
              zkServer.notifyAll();
            }
          }
        });
        System.out.println("zookeeper server started on port " + zkServer.getPort());
        zkServer.wait();
      }
    }
  };

  protected static Command START_MASTER_COMMAND = new Command("startMaster", "[-e] [-ne]",
          "Starts a local master. -e & -ne for embedded and non-embedded zk-server (overriding configuration)") {

    private boolean _embeddedMode;

    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap)
            throws Exception {
      if (optionMap.containsKey("-e")) {
        _embeddedMode = true;
      } else if (optionMap.containsKey("-ne")) {
        _embeddedMode = false;
      } else {
        _embeddedMode = zkConf.isEmbedded();
      }
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
      final Master master;
      if (_embeddedMode) {
        LOG.info("starting embedded zookeeper server...");
        ZkServer zkServer = ZkKattaUtil.startZkServer(zkConf);
        master = new Master(new InteractionProtocol(zkServer.getZkClient(), zkConf), zkServer);
      } else {
        ZkClient zkClient = ZkKattaUtil.startZkClient(zkConf, 30000);
        master = new Master(new InteractionProtocol(zkClient, zkConf), true);
      }
      master.start();

      synchronized (master) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            synchronized (master) {
              master.shutdown();
              master.notifyAll();
            }
          }
        });
        master.wait();
      }
    }
  };

  protected static Command START_NODE_COMMAND = new ProtocolCommand("startNode", "[-c <serverClass>]",
          "Starts a local node") {

    private NodeConfiguration _nodeConfiguration;
    private INodeManaged _server = null;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
      _nodeConfiguration = new NodeConfiguration();
      String serverClassName;
      if (optionMap.containsKey("-c")) {
        serverClassName = optionMap.get("-c");
      } else {
        serverClassName = _nodeConfiguration.getServerClassName();
      }

      Class<?> serverClass = loadClass(serverClassName, INodeManaged.class);
      try {
        _server = (INodeManaged) serverClass.newInstance();
      } catch (Exception e) {
        throw new IllegalStateException("could not create instance of class '" + serverClassName + "': "
                + e.getMessage());
      }
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      final Node node = new Node(protocol, _nodeConfiguration, _server);
      node.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          node.shutdown();
        }
      });
      node.join();
    }

  };

  protected static Command LIST_NODES_COMMAND = new ProtocolCommand("listNodes", "", "Lists all nodes") {

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
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

    private boolean _detailedView;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      _detailedView = optionMap.containsKey("-d");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) {
      final Table table;
      if (!_detailedView) {
        table = new Table(new String[] { "Name", "Status", "Replication State", "Path", "Shards", "Entries",
                "Disk Usage" });
      } else {
        table = new Table(new String[] { "Name", "Status", "Replication State", "Path", "Shards", "Entries",
                "Disk Usage", "Replication Count" });
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
        String state = "DEPLOYED";
        String replicationState = "BALANCED";
        if (indexMD.hasDeployError()) {
          state = "ERROR";
          replicationState = "-";
        } else {
          ReplicationReport report = protocol.getReplicationReport(indexMD);
          if (report.isUnderreplicated()) {
            replicationState = "UNDERREPLICATED";
          } else if (report.isOverreplicated()) {
            replicationState = "OVERREPLICATED";
          }

        }
        if (!_detailedView) {
          table.addRow(index, state, replicationState, indexMD.getPath(), shards.size(), entries, indexBytes);
        } else {
          table.addRow(index, state, replicationState, indexMD.getPath(), shards.size(), entries, indexBytes, indexMD
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

    private OutputType _outputType;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      if (args.length < 2) {
        throw new IllegalArgumentException("no output type specified");
      }
      if (parseType(args[1]) == null) {
        throw new IllegalArgumentException("need to specify one of " + Arrays.asList(OutputType.values())
                + " as output type");
      }
      _outputType = parseType(args[1]);

    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      new MetricLogger(_outputType, protocol).join();
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

    private int _port = 8080;
    private File _war;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap) {
      if (optionMap.containsKey("-war")) {
        _war = new File(optionMap.get("-war"));
      }
      if (optionMap.containsKey("-port")) {
        _port = Integer.parseInt(optionMap.get("-port"));
      }
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
      List<String> paths = new ArrayList<String>();
      if (_war != null) {
        paths.add(_war.getAbsolutePath());
      } else {
        paths.add(".");
        paths.add("./extras/katta.gui");
      }

      WebApp app = new WebApp(paths.toArray(new String[paths.size()]), _port);
      app.startWebServer();
    }
  };

  protected static Command SHOW_STRUCTURE_COMMAND = new ProtocolCommand("showStructure", "[-d]",
          "Shows the structure of a Katta installation. -d for detailed view.") {

    private boolean _detailedView;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      _detailedView = optionMap.containsKey("-d");
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      protocol.showStructure(_detailedView);
    }
  };

  protected static Command CHECK_COMMAND = new ProtocolCommand("check", "", "Analyze index/shard/node status") {

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
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
    public void execute(ZkConfiguration zkConf) throws Exception {
      VersionInfo versionInfo = new VersionInfo();
      System.out.println("Katta '" + versionInfo.getVersion() + "'");
      System.out.println("Git-Revision '" + versionInfo.getRevision() + "'");
      System.out.println("Compiled by '" + versionInfo.getCompiledBy() + "' on '" + versionInfo.getCompileTime() + "'");
    }
  };

  protected static Command ADD_INDEX_COMMAND = new ProtocolCommand("addIndex",
          "<index name> <path to index> [<replication level>]", "Add a index to Katta") {

    private String _name;
    private String _path;
    private int _replicationLevel = 3;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 3);
      _name = args[1];
      _path = args[2];
      if (args.length >= 4) {
        _replicationLevel = Integer.parseInt(args[3]);
      }
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      addIndex(protocol, _name, _path, _replicationLevel);
    }

  };

  protected static Command REMOVE_INDEX_COMMAND = new ProtocolCommand("removeIndex", "<index name>",
          "Remove a index from Katta") {
    private String _indexName;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 2);
      _indexName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      removeIndex(protocol, _indexName);
      System.out.println("undeployed index '" + _indexName + "'");
    }

  };

  protected static Command REDEPLOY_INDEX_COMMAND = new ProtocolCommand("redeployIndex", "<index name>",
          "Undeploys and deploys an index") {

    private String _indexName;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 2);
      _indexName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      IndexMetaData indexMD = protocol.getIndexMD(_indexName);
      if (indexMD == null) {
        throw new IllegalArgumentException("index '" + _indexName + "' does not exist");
      }
      removeIndex(protocol, _indexName);
      Thread.sleep(5000);
      addIndex(protocol, _indexName, indexMD.getPath(), indexMD.getReplicationLevel());
    }
  };

  protected static Command LIST_ERRORS_COMMAND = new ProtocolCommand("listErrors", "<index name>",
          "Lists all deploy errors for a specified index") {

    private String _indexName;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 2);
      _indexName = args[1];
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      IndexMetaData indexMD = protocol.getIndexMD(_indexName);
      if (indexMD == null) {
        throw new IllegalArgumentException("index '" + _indexName + "' does not exist");
      }
      if (!indexMD.hasDeployError()) {
        System.out.println("No error for index '" + _indexName + "'");
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

    private String[] _indexNames;
    private String _query;
    private int _count;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 3);
      _indexNames = args[1].split(",");
      _query = args[2];
      if (args.length > 3) {
        _count = Integer.parseInt(args[3]);
      }
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      if (_count > 0) {
        search(_indexNames, _query, _count);
      } else {
        search(_indexNames, _query);
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

    private String _input;
    private String _output;
    private int _wordsPerDoc;
    private int _indexSize;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 4);
      _input = args[1];
      _output = args[2];
      _wordsPerDoc = Integer.parseInt(args[3]);
      _indexSize = Integer.parseInt(args[4]);
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      SampleIndexGenerator sampleIndexGenerator = new SampleIndexGenerator();
      sampleIndexGenerator.createIndex(_input, _output, _wordsPerDoc, _indexSize);
    }

  };

  protected static Command LOADTEST_COMMAND = new ProtocolCommand(
          "loadtest",
          "<zkRootPath> <nodeCount> <startQueryRate> <endQueryRate> <rateStep> <durationPerIteration> <indexName> <query-file> <resultFolder> <typeWithParameters> ",
          "Starts a load test on a katta cluster with the given zkRootPath. The query rate is in queries per second. The durationPerIteration is in milliseconds. The resultFolder will be created on the master host. typeWithParameters is one of 'lucene <maxHits>' | 'mapfile'") {

    private LoadTestMasterOperation _masterOperation;
    private File _resultFolder;

    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap) {
      validateMinArguments(args, 11);
      String zkRootPath = args[1];
      int nodeCount = Integer.parseInt(args[2]);
      int startQueryRate = Integer.parseInt(args[3]);
      int endQueryRate = Integer.parseInt(args[4]);
      int rateStep = Integer.parseInt(args[5]);
      long durationPerIteration = Integer.parseInt(args[6]);
      String indexName = args[7];
      File queryFile = new File(args[8]);
      _resultFolder = new File(args[9]);
      String type = args[10];

      if (!queryFile.exists()) {
        throw new IllegalStateException("query file '" + queryFile.getAbsolutePath() + "' does not exists");
      }
      AbstractQueryExecutor queryExecutor;
      String[] indices = new String[] { indexName };
      String[] queries = readQueries(queryFile);
      if (type.equalsIgnoreCase("lucene")) {
        int maxHits = Integer.parseInt(args[11]);
        ZkConfiguration searchClusterZkConf = new ZkConfiguration();
        searchClusterZkConf.setZKServers(zkConf.getZKServers());
        searchClusterZkConf.setZKRootPath(zkRootPath);
        queryExecutor = new LuceneSearchExecutor(indices, queries, searchClusterZkConf, maxHits);
      } else if (type.equalsIgnoreCase("mapfile")) {
        queryExecutor = new MapfileAccessExecutor(indices, queries, zkConf);
      } else {
        throw new IllegalStateException("type '" + type + "' unknown");
      }

      _masterOperation = new LoadTestMasterOperation(nodeCount, startQueryRate, endQueryRate, rateStep,
              durationPerIteration, queryExecutor, _resultFolder);
    }

    @Override
    public void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
      _masterOperation.registerCompletion(protocol);
      protocol.addMasterOperation(_masterOperation);
      long startTime = System.currentTimeMillis();
      System.out.println("load test triggered - waiting on completion...");
      _masterOperation.joinCompletion(protocol);
      System.out.println("load test complete - took " + (System.currentTimeMillis() - startTime) + " ms");
      System.out.println("find the results in '" + _resultFolder.getAbsolutePath()
              + "' on the master or inspect the master logs if the results are missing");
    }

    private String[] readQueries(File queryFile) {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        List<String> lines = new ArrayList<String>();
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (!line.equals("")) {
            lines.add(line);
          }
        }
        return lines.toArray(new String[lines.size()]);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read query file " + queryFile + ".", e);
      }
    }

  };

  protected static Command RUN_CLASS_COMMAND = new Command("runclass", "<className>", "runs a custom class") {

    private Class<?> _clazz;
    private Method _method;
    private String[] _methodArgs;

    @Override
    protected void parseArguments(ZkConfiguration zkConf, String[] args, java.util.Map<String, String> optionMap)
            throws Exception {
      validateMinArguments(args, 2);
      _clazz = loadClass(args[1], Object.class);
      _method = _clazz.getMethod("main", args.getClass());
      if (_method == null) {
        throw new IllegalArgumentException("class " + _clazz.getName() + " doesn't have a main method");
      }
      _methodArgs = Arrays.copyOfRange(args, 2, args.length);
    }

    @Override
    public void execute(ZkConfiguration zkConf) throws Exception {
      _method.invoke(null, new Object[] { _methodArgs });
    }

  };

  static {
    COMMANDS.add(START_ZK_COMMAND);
    COMMANDS.add(START_MASTER_COMMAND);
    COMMANDS.add(START_NODE_COMMAND);
    COMMANDS.add(LIST_INDICES_COMMAND);
    COMMANDS.add(LIST_NODES_COMMAND);
    COMMANDS.add(LIST_ERRORS_COMMAND);
    COMMANDS.add(ADD_INDEX_COMMAND);
    COMMANDS.add(REMOVE_INDEX_COMMAND);
    COMMANDS.add(REDEPLOY_INDEX_COMMAND);
    COMMANDS.add(CHECK_COMMAND);
    COMMANDS.add(VERSION_COMMAND);
    COMMANDS.add(SHOW_STRUCTURE_COMMAND);
    COMMANDS.add(START_GUI_COMMAND);
    COMMANDS.add(LOG_METRICS_COMMAND);
    COMMANDS.add(GENERATE_INDEX_COMMAND);
    COMMANDS.add(SEARCH_COMMAND);
    COMMANDS.add(LOADTEST_COMMAND);
    COMMANDS.add(RUN_CLASS_COMMAND);

    Set<String> commandStrings = new HashSet<String>();
    for (Command command : COMMANDS) {
      if (!commandStrings.add(command.getCommand())) {
        throw new IllegalStateException("duplicated command sting " + command.getCommand());
      }
    }
  }

  static abstract class Command {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(Command.class);

    private final String _command;
    private final String _parameterString;
    private final String _description;

    public Command(String command, String parameterString, String description) {
      _command = command;
      _parameterString = parameterString;
      _description = description;
    }

    public final void parseArguments(String[] args) throws Exception {
      parseArguments(new ZkConfiguration(), args, parseOptionMap(args));
    }

    public final void parseArguments(ZkConfiguration zkConf, String[] args) throws Exception {
      parseArguments(zkConf, args, parseOptionMap(args));
    }

    @SuppressWarnings("unused")
    protected void parseArguments(ZkConfiguration zkConf, String[] args, Map<String, String> optionMap)
            throws Exception {
      // subclasses may override
    }

    public void execute() throws Exception {
      execute(new ZkConfiguration());
    }

    protected abstract void execute(ZkConfiguration zkConf) throws Exception;

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
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(ProtocolCommand.class);

    public ProtocolCommand(String command, String parameterString, String description) {
      super(command, parameterString, description);
    }

    @Override
    public final void execute(ZkConfiguration zkConf) throws Exception {
      ZkClient zkClient = new ZkClient(zkConf.getZKServers());
      InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);
      execute(zkConf, protocol);
      protocol.disconnect();
    }

    protected abstract void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception;
  }

  private static class Table {
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(Table.class);

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
    /**
     * Logger for this class
     */
    private static final Logger logger = Logger.getLogger(CounterMap.class);

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
