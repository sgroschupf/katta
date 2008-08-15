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
package net.sf.katta.node;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.master.IPaths;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.Logger;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

public class Node implements ISearch, IZkReconnectListener {

  public static final long _protocolVersion = 0;
  @SuppressWarnings("unused")
  private static final long versionID = 0;

  private static final int BUFFER = 4096;

  protected ZKClient _zkClient;
  private Server _rpcServer;
  private KattaMultiSearcher _searcher;

  private final QueryParser _luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());

  protected String _name;
  protected File _shardFolder;
  protected final ArrayList<String> _deployedShards = new ArrayList<String>();

  private final Timer _timer;
  protected final long _startTime;
  protected long _queryCounter;

  private final NodeConfiguration _configuration;

  private NodeState _currentState;

  public static enum NodeState {
    STARTING, RECONNECTING, IN_SERVICE, LOST;
  }

  public Node(final ZKClient zkClient) {
    this(zkClient, new NodeConfiguration());
  }

  public Node(final ZKClient zkClient, final NodeConfiguration configuration) {
    _zkClient = zkClient;
    _configuration = configuration;
    _startTime = System.currentTimeMillis();
    _timer = new Timer("QueryCounter", true);

    _shardFolder = _configuration.getShardFolder();
    _zkClient.subscribeReconnects(this);
  }

  /**
   * Boots the node
   * 
   * @throws KattaException
   */
  public void start() throws KattaException {
    Logger.debug("Starting node...");
    if (!_shardFolder.exists()) {
      _shardFolder.mkdirs();
    }

    Logger.debug("Starting rpc server...");
    _name = startRPCServer(_configuration);

    Logger.debug("Starting zk client...");
    if (!_zkClient.isStarted()) {
      _zkClient.start(30000);
    }
    ArrayList<String> shards = announceNode(_zkClient);
    updateStatus(NodeState.STARTING);
    checkAndDeployExistingShards(NodeState.STARTING, shards);

    _searcher = new KattaMultiSearcher(_name);

    Logger.info("Started: " + _name + "...");

    if (_rpcServer != null) {
      try {
        _rpcServer.start();
      } catch (final IOException e) {
        throw new RuntimeException("Failed to start node server.", e);
      }
    } else {
      throw new RuntimeException("tried 10000 ports and no one is free...");
    }
    updateStatus(NodeState.IN_SERVICE);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  public void shutdown() {
    // TODO jz: do decommission first?
    _timer.cancel();
    _zkClient.close();
    _rpcServer.stop();
  }

  public String getName() {
    return _name;
  }

  public void join() {
    try {
      _rpcServer.join();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to join node", e);
    }
  }

  public ArrayList<String> getDeployShards() {
    return _deployedShards;
  }

  /*
   * Writes node ephemeral data into zookeeper
   */
  private ArrayList<String> announceNode(ZKClient client) throws KattaException {
    Logger.debug("Announces node " + _name);
    final NodeMetaData metaData = new NodeMetaData(_name, "booting", true, _startTime);
    final String nodePath = IPaths.NODES + "/" + _name;
    final String nodeToShardPath = IPaths.NODE_TO_SHARD + "/" + _name;
    if (client.exists(nodePath)) {
      Logger.debug("Old node for this host detected, will be removed...");
      client.delete(nodePath);
    }
    client.createEphemeral(nodePath, metaData);
    if (!client.exists(nodeToShardPath)) {
      client.create(nodeToShardPath);
    }
    Logger.debug("Add shard listener in node.");
    return client.subscribeChildChanges(nodeToShardPath, new ShardListener());
  }

  public void handleReconnect() throws KattaException {
    ArrayList<String> shardsToServe = announceNode(_zkClient);
    updateStatus(NodeState.RECONNECTING);
    Logger.info("My old shards to serve: " + shardsToServe);
    checkAndDeployExistingShards(NodeState.RECONNECTING, shardsToServe);
    updateStatus(NodeState.STARTING);
  }

  /*
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of node.server.port.start + 10000
   */
  private String startRPCServer(final NodeConfiguration configuration) {
    int serverPort = configuration.getStartPort();
    final String hostName = NetworkUtil.getLocalhostName();
    for (int i = serverPort; i < (serverPort + 10000); i++) {
      try {
        Logger.info("starting RPC server on : " + hostName);
        _rpcServer = RPC.getServer(this, "0.0.0.0", i, new Configuration());
        serverPort = i;
        break;
      } catch (final BindException e) {
        // try again
      } catch (final IOException e) {
        throw new RuntimeException("unable to start node server", e);
      }
    }
    return hostName + ":" + serverPort;
  }

  /*
   * When starting a node there might be shards assigned that are still on local
   * hhd. Therefore we compare what is assigned, what can be re-used and which
   * shards we need to load from the remote hdd.
   */
  private void checkAndDeployExistingShards(NodeState nodeState, final ArrayList<String> shardsToDeploy)
      throws KattaException {
    synchronized (_shardFolder) {
      final String[] localShards = _shardFolder.list(new FilenameFilter() {
        public boolean accept(final File dir, final String name) {
          return !name.startsWith(".");
        }
      });
      if (Logger.isDebug()) {
        Logger.debug("Getted local shard folder list.");
      }
      // in case not shards are assigned we want to remove local shards.
      if (shardsToDeploy == null || shardsToDeploy.size() == 0) {
        Logger.debug("Remove all local shards, because no shard has to be deployed.");
        LocalFileSystem localFileSystem;
        try {
          localFileSystem = FileSystem.getLocal(new Configuration());
          for (final String localShard : localShards) {
            final Path localShardPath = new Path(_shardFolder.getAbsolutePath(), localShard);
            if (localFileSystem.exists(localShardPath)) {
              if (!localFileSystem.delete(localShardPath)) {
                throw new RuntimeException("unable to delete local shard: " + localShardPath.toString());
              }
            }
          }
        } catch (final IOException e) {
          throw new KattaException("Failed to use local hadoop file system.", e);
        }
      }
      // remove those we do not need anymore
      final List<String> localShardList = Arrays.asList(localShards);
      List<String> removed = ComparisonUtil.getRemoved(localShardList, shardsToDeploy);
      removeShards(removed);
      if (Logger.isDebug()) {
        Logger.debug("No longer needed shards removed: " + removed);
      }

      // now only download those we do not yet have local or we can't deploy
      if (shardsToDeploy != null && shardsToDeploy.size() != 0) {
        final HashSet<String> existingShards = new HashSet<String>();
        existingShards.addAll(localShardList);

        for (final String shardName : shardsToDeploy) {
          DeployedShard deployedShard = null;
          final AssignedShard assignedShard = getAssignedShard(shardName);

          File localShardFolder = null;
          try {

            // load first or use local file
            if (!existingShards.contains(shardName)) {
              Logger.debug("Shard '" + shardName + "' has to be deployed.");
              localShardFolder = loadAndUnzipShard(nodeState, assignedShard);
            } else {
              Logger.debug("Shard '" + shardName + "' already deployed.");
              localShardFolder = new File(_shardFolder, shardName);
            }
            // deploy and announce
            final int numOfDocs = deployShard(shardName, localShardFolder);
            deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), numOfDocs);
          } catch (final Exception e) {
            updateStatusWithError(e);
            if (localShardFolder != null) {
              if (localShardFolder.exists()) {
                deleteFolder(localShardFolder);
              }
            }
            if (Logger.isError()) {
              Logger.error("Unable to load shard:", e);
            }
            deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), 0);
            deployedShard.setErrorMsg(e.getMessage());
          } finally {
            announceShard(deployedShard);
          }
        }
      }
    }
  }

  protected void deployAndAnnounceShards(NodeState state, final List<String> shardsToServe) {
    for (final String shardName : shardsToServe) {
      deployAndAnnounceShard(state, shardName);
    }
  }

  /*
   * Loads, deploys and announce a single fresh assigned shard.
   */
  private void deployAndAnnounceShard(NodeState state, final String shardName) {
    DeployedShard deployedShard = null;
    File localShardFolder = null;
    AssignedShard assignedShard = null;
    try {
      assignedShard = getAssignedShard(shardName);
      localShardFolder = loadAndUnzipShard(state, assignedShard);
      final int numOfDocs = deployShard(shardName, localShardFolder);
      deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), numOfDocs);
    } catch (final Exception e) {
      if (localShardFolder != null) {
        if (localShardFolder.exists()) {
          deleteFolder(localShardFolder);
        }
      }
      if (Logger.isError()) {
        Logger.error("Unable to load shard:", e);
      }
      updateStatusWithError(e);
      deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), 0);
      deployedShard.setErrorMsg(e.getMessage());
    } finally {
      announceShard(deployedShard);
    }
  }

  /*
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content.
   */
  private File loadAndUnzipShard(NodeState nodeState, final AssignedShard assignedShard) {
    final String shardKey = assignedShard.getShardName();
    final String shardPath = assignedShard.getShardPath();
    URI uri;
    try {
      uri = new URI(shardPath);
      final FileSystem fileSystem = FileSystem.get(uri, new Configuration());
      final Path path = new Path(shardPath);
      boolean isZip = false;
      if (fileSystem.isFile(path) && shardPath.endsWith(".zip")) {
        isZip = true;
      }
      synchronized (_shardFolder) {
        final File shardZip = new File(_shardFolder, assignedShard.getShardName() + ".zip");
        final File shardFolder = new File(_shardFolder, assignedShard.getShardName());

        if (isZip) {
          final Path dest = new Path(shardZip.getAbsolutePath());
          updateStatus(nodeState, "copy shard: " + shardKey);
          // make sure we overwrite cleanly
          if (fileSystem.exists(dest)) {
            fileSystem.delete(dest);
          }
          fileSystem.copyToLocalFile(path, dest);
          // decompress
          updateStatus(nodeState, "decompressing shard: " + shardKey);
          decompress(shardZip, shardFolder);
          // remove zip
          shardZip.delete();
        } else {
          final Path dest = new Path(shardFolder.getAbsolutePath());
          // make sure we overwrite cleanly
          if (fileSystem.exists(dest)) {
            fileSystem.delete(dest);
          }
          updateStatus(nodeState, "copy shard: " + shardKey);
          fileSystem.copyToLocalFile(path, dest);

        }
        updateStatus(nodeState, "Load and Unzip Shard ready.");
        return shardFolder;
      }
    } catch (final URISyntaxException e) {
      final String msg = "Can not parse uri for path: " + assignedShard.getShardPath();
      Logger.error(msg, e);
      updateStatusWithError(msg);
      updateShardStatusInNode(assignedShard.getIndexName(), msg);

      throw new RuntimeException(msg, e);
    } catch (final IOException e) {
      final String msg = "Can not load shard: " + assignedShard.getShardPath();
      Logger.error(msg, e);
      updateStatusWithError(msg);
      updateShardStatusInNode(assignedShard.getIndexName(), msg);
      throw new RuntimeException(msg, e);
    }
  }

  /*
   * Simply unzips the content though we remove the first level folder, since we
   * already created a shard folder where we want to have the content extracted
   * to.
   */
  private void decompress(final File source, final File target) {
    try {
      target.mkdirs();
      BufferedOutputStream dest = null;
      final FileInputStream fis = new FileInputStream(source);
      final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {

        Logger.debug("Extracting:   " + entry + " from '" + source.getAbsolutePath() + "'");
        // we need to remove the first element of the path since the
        // folder was compressed but we only want the folders content
        final String entryPath = entry.getName();
        final int indexOf = entryPath.indexOf("/");
        final String cleanUpPath = entryPath.substring(indexOf + 1, entryPath.length());
        final File targetFile = new File(target, cleanUpPath);
        if (entry.isDirectory()) {
          targetFile.mkdirs();
        } else {
          int count;
          final byte data[] = new byte[BUFFER];
          final FileOutputStream fos = new FileOutputStream(targetFile);
          dest = new BufferedOutputStream(fos, BUFFER);
          while ((count = zis.read(data, 0, BUFFER)) != -1) {
            dest.write(data, 0, count);
          }
          dest.flush();
          dest.close();
        }
      }
      zis.close();

    } catch (final Exception e) {
      throw new RuntimeException("unable to expand upgrade files", e);
    }
  }

  /*
   * Creates an index search and adds it to the KattaMultiSearch
   */
  private int deployShard(final String shardName, final File localShardFolder) {
    IndexSearcher indexSearcher;
    try {
      indexSearcher = new IndexSearcher(localShardFolder.getAbsolutePath());
      _searcher.addShard(shardName, indexSearcher);
      _deployedShards.add(shardName);
      return indexSearcher.maxDoc();
    } catch (final Exception e) {
      String msg = "Shard index " + shardName + " can't be started";
      throw new RuntimeException(msg, e);
    }
  }

  /*
   * Reads AssignedShard data from ZooKeeper
   */
  private AssignedShard getAssignedShard(final String shardName) throws KattaException {
    final AssignedShard assignedShard = new AssignedShard();
    _zkClient.readData(IPaths.NODE_TO_SHARD + "/" + _name + "/" + shardName, assignedShard);
    return assignedShard;
  }

  /*
   * Announce in zookeeper node is serving this shard,
   */

  private void announceShard(final DeployedShard deployedShard) {
    try {
      final String shardPath = IPaths.SHARD_TO_NODE + "/" + deployedShard.getShardName();
      // announce that this node serves this shard now...
      final String nodePath = shardPath + "/" + _name;
      if (!_zkClient.exists(nodePath)) {
        _zkClient.createEphemeral(nodePath, deployedShard);
      } else {
        _zkClient.writeData(nodePath, deployedShard);
      }
    } catch (final Exception e) {
      if (Logger.isError()) {
        Logger.error("Unable to serve Shard: " + deployedShard, e);
      }
    }
  }

  protected void removeShards(final List<String> shardsToRemove) {
    Logger.info("Removing shards: " + shardsToRemove);
    for (final String shardName : shardsToRemove) {
      removeShard(shardName);
    }
  }

  /*
   * Removes the shard from KattaMultiSearcher, our list of deployed Shards and
   * removes the index from local hdd.
   */
  private void removeShard(final String shardName) {
    try {
      if (Logger.isInfo()) {
        Logger.info("Removing shard: " + shardName);
      }
      _searcher.removeShard(shardName);
      _deployedShards.remove(shardName);
      final String shardPath = IPaths.SHARD_TO_NODE + "/" + shardName;
      final String nodePath = shardPath + "/" + _name;
      if (_zkClient.exists(nodePath)) {
        _zkClient.delete(nodePath);
      } // remove node serving it.
      if (_zkClient.exists(shardPath) && _zkClient.getChildren(shardPath).size() == 0) {
        // this was the last node
        _zkClient.delete(shardPath);
      }
      synchronized (_shardFolder) {
        deleteFolder(new File(_shardFolder, shardName));
      }
    } catch (final Exception e) {
      if (Logger.isError()) {
        Logger.error("Failed to remove local shard: " + shardName, e);
      }
    }
  }

  private boolean deleteFolder(final File dir) {
    if (dir.isDirectory()) {
      final String[] children = dir.list();
      if (children != null) {
        for (int i = 0; i < children.length; i++) {
          final boolean success = deleteFolder(new File(dir, children[i]));
          if (!success) {
            return false;
          }
        }
      }
    }
    return dir.delete();
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#search(net.sf.katta.node.IQuery,
   * net.sf.katta.node.DocumentFrequenceWritable, java.lang.String[])
   */
  public HitsMapWritable search(final IQuery query, final DocumentFrequenceWritable freqs, final String[] shards)
      throws IOException {
    return search(query, freqs, shards, Integer.MAX_VALUE - 1);
  }

  public HitsMapWritable search(final IQuery query, final DocumentFrequenceWritable freqs, final String[] shards,
      final int count) throws IOException {
    if (Logger.isDebug()) {
      Logger.debug("You are searching with the query: '" + query.getQuery() + "'");
    }

    Query luceneQuery;
    try {
      luceneQuery = _luceneQueryParser.parse(query.getQuery());
    } catch (final ParseException e) {

      final String msg = "Failed to parse query: " + query.getQuery();
      if (Logger.isError()) {
        Logger.error(msg, e);
      }
      final IOException exception = new IOException(msg);
      exception.setStackTrace(e.getStackTrace());
      throw exception;
    }
    if (Logger.isDebug()) {
      Logger.debug("Lucene query: " + luceneQuery.toString());
    }

    long completeSearchTime = 0;
    final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(_name);
    if (_searcher != null) {
      long start = 0;
      if (Logger.isDebug()) {
        start = System.currentTimeMillis();
      }
      _searcher.search(luceneQuery, freqs, shards, result, count);
      if (Logger.isDebug()) {
        final long end = System.currentTimeMillis();
        Logger.debug("Search took " + (end - start) / 1000.0 + "sec.");
        completeSearchTime += (end - start);
      }
    } else {
      Logger.error("No searcher for index found on '" + _name + "'.");
    }
    if (Logger.isDebug()) {
      Logger.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
      final DataOutputBuffer buffer = new DataOutputBuffer();
      result.write(buffer);
      Logger.debug("Result size to transfer: " + buffer.getLength());
    }
    return result;
  }

  public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
    return _protocolVersion;
  }

  public DocumentFrequenceWritable getDocFreqs(final IQuery input, final String[] shards) throws IOException {
    Query luceneQuery;
    try {
      luceneQuery = _luceneQueryParser.parse(input.getQuery());
    } catch (final ParseException e) {
      final String msg = "Unable to parse Query: " + input.getQuery();
      final IOException exception = new IOException(msg);
      exception.setStackTrace(e.getStackTrace());
      if (Logger.isError()) {
        Logger.error(msg, e);
      }
      throw exception;
    }

    final Query rewrittenQuery = _searcher.rewrite(luceneQuery, shards);
    final DocumentFrequenceWritable docFreqs = new DocumentFrequenceWritable();

    final HashSet<Term> termSet = new HashSet<Term>();
    rewrittenQuery.extractTerms(termSet);
    final java.util.Iterator<Term> termIterator = termSet.iterator();
    int numDocs = 0;
    for (final String shard : shards) {
      while (termIterator.hasNext()) {
        final Term term = termIterator.next();
        final int docFreq = _searcher.docFreq(shard, term);
        docFreqs.put(term.field(), term.text(), docFreq);
      }
      numDocs += _searcher.getNumDoc(shard);
    }
    docFreqs.setNumDocs(numDocs);
    return docFreqs;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#getDetails(java.lang.String, int)
   */
  @SuppressWarnings("unchecked")
  public MapWritable getDetails(final String shard, final int docId) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = _searcher.doc(shard, docId);
    final List<Fieldable> fields = doc.getFields();
    for (final Fieldable field : fields) {
      final String name = field.name();
      if (field.isBinary()) {
        final byte[] binaryValue = field.binaryValue();
        result.put(new Text(name), new BytesWritable(binaryValue));
      } else {
        final String stringValue = field.stringValue();
        result.put(new Text(name), new Text(stringValue));
      }
    }
    return result;
  }

  public MapWritable getDetails(final String shard, final int docId, final String[] fieldNames) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = _searcher.doc(shard, docId);
    for (final String fieldName : fieldNames) {
      final Field field = doc.getField(fieldName);
      if (field != null) {
        if (field.isBinary()) {
          final byte[] binaryValue = field.binaryValue();
          result.put(new Text(fieldName), new BytesWritable(binaryValue));
        } else {
          final String stringValue = field.stringValue();
          result.put(new Text(fieldName), new Text(stringValue));
        }
      }
    }
    return result;
  }

  public int getResultCount(final IQuery query, final String[] shards) throws IOException {
    final DocumentFrequenceWritable docFreqs = getDocFreqs(query, shards);
    return search(query, docFreqs, shards, 1).getTotalHits();
  }

  @Override
  protected void finalize() throws Throwable {
    shutdown();
  }

  private void updateStatus(NodeState state) {
    updateStatus(state, null);
  }

  private synchronized void updateStatus(NodeState state, final String statusMsg) {
    _currentState = state;
    final String path = IPaths.NODES + "/" + _name;
    final NodeMetaData metaData = new NodeMetaData();
    try {
      _zkClient.readData(path, metaData);
      if (statusMsg == null) {
        metaData.setStatus(state.name());
      } else {
        metaData.setStatus(state + ": " + statusMsg);
      }
      metaData.setStarting(state == NodeState.STARTING);
      _zkClient.writeData(path, metaData);
    } catch (KattaException e) {
      Logger.error("Cannot update node status.", e);
    }
  }

  private void updateStatusWithError(Exception exception) {
    // TODO jz: stringify exception / multiple exceptions ?
    updateStatusWithError(exception.getMessage());
  }

  private void updateStatusWithError(String errorString) {
    final String path = IPaths.NODES + "/" + _name;
    final NodeMetaData metaData = new NodeMetaData();
    try {
      _zkClient.readData(path, metaData);
      metaData.setException(errorString);
      _zkClient.writeData(path, metaData);
    } catch (KattaException e) {
      Logger.error("Cannot update node status.", e);
    }
  }

  private void updateShardStatusInNode(final String shardName, final String errorMsg) {
    final String nodePath = IPaths.SHARD_TO_NODE + "/" + shardName + "/" + _name;
    DeployedShard deployedShard = new DeployedShard();
    try {
      if (_zkClient.exists(nodePath)) {
        _zkClient.readData(nodePath, deployedShard);
        if (null == errorMsg) {
          deployedShard.cleanError();
        } else {
          deployedShard.setErrorMsg(errorMsg);
        }
        _zkClient.writeData(nodePath, deployedShard);
      }
    } catch (final Exception e) {
      if (Logger.isError()) {
        Logger.error("Failed to write shard status." + deployedShard, e);
      }
    }
  }

  /*
   * Listens to events within the nodeToShard zookeeper folder. Those events are
   * fired if a shard is assigned or removed for this node.
   */
  protected class ShardListener implements IZkChildListener {

    public void handleChildChange(String parentPath) throws KattaException {
      Logger.debug("Add/Remove shard.");
      synchronized (_zkClient.getSyncMutex()) {
        if (Logger.isDebug()) {
          Logger.debug("ShardListener.process()" + _name);
        }
        List<String> newList;
        try {
          newList = _zkClient.getChildren(parentPath);
          final List<String> shardsToRemove = ComparisonUtil.getRemoved(_deployedShards, newList);
          removeShards(shardsToRemove);
          final List<String> shardsToServe = ComparisonUtil.getNew(_deployedShards, newList);
          deployAndAnnounceShards(NodeState.IN_SERVICE, shardsToServe);
        } catch (final KattaException e) {
          throw new RuntimeException("Failed to read zookeeper information");
        } finally {
          _zkClient.getSyncMutex().notifyAll();
        }
      }
    }
  }

  /*
   * A Thread that updates the status of the node within zookeeper.
   */
  protected class StatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_name != null) {
        long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
        time = Math.max(time, 1);
        final float qpm = (float) _queryCounter / time;
        final NodeMetaData metaData = new NodeMetaData();
        final String path = IPaths.NODES + "/" + _name;
        try {
          if (_zkClient.exists(path)) {
            _zkClient.readData(path, metaData);
            metaData.setQueriesPerMinute(qpm);
            _zkClient.writeData(path, metaData);
          }
        } catch (final KattaException e) {
          Logger.error("Failed to update node status (StatusUpdater).", e);
        }
      }
    }
  }

  public NodeState getState() {
    return _currentState;
  }

}
