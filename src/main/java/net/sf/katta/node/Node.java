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

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.ShardError;
import net.sf.katta.util.CollectionUtil;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.util.NodeConfiguration;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkReconnectListener;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

public class Node implements ISearch, IZkReconnectListener {

  protected final static Logger LOG = Logger.getLogger(Node.class);

  public static final long _protocolVersion = 0;

  protected ZKClient _zkClient;
  private Server _rpcServer;
  private KattaMultiSearcher _searcher;

  private final QueryParser _luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());

  protected String _nodeName;
  protected int _searchServerPort;
  protected File _shardsFolder;
  // contains the deploy errors two
  protected final Set<String> _deployedShards = new HashSet<String>();

  private final Timer _timer;
  protected final long _startTime = System.currentTimeMillis();
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
    _timer = new Timer("QueryCounter", true);

    _shardsFolder = _configuration.getShardFolder();
    _zkClient.subscribeReconnects(this);
  }

  /**
   * Boots the node
   * 
   * @throws KattaException
   */
  public void start() throws KattaException {
    LOG.debug("Starting node...");
    if (!_shardsFolder.exists()) {
      _shardsFolder.mkdirs();
    }
    if (!_shardsFolder.exists()) {
      throw new IllegalStateException("could not create slocal shard folder '" + _shardsFolder.getAbsolutePath() + "'");
    }

    LOG.debug("Starting rpc search server...");
    _nodeName = startRPCServer(_configuration);

    LOG.debug("Starting zk client...");
    if (!_zkClient.isStarted()) {
      _zkClient.start(30000);
    }
    cleanupLocalShardFolder();
    announceNode(NodeState.STARTING);
    startShardServing(false);

    LOG.info("Started node: " + _nodeName + "...");
    updateStatus(NodeState.IN_SERVICE);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  public void handleReconnect() throws KattaException {
    announceNode(NodeState.RECONNECTING);
    cleanupLocalShardFolder();
    startShardServing(true);
    updateStatus(NodeState.IN_SERVICE);
  }

  private void cleanupLocalShardFolder() throws KattaException {
    String node2ShardRootPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    List<String> shardsToServe = Collections.EMPTY_LIST;
    if (_zkClient.exists(node2ShardRootPath)) {
      shardsToServe = _zkClient.getChildren(node2ShardRootPath);
    }
    List<String> localShards = Arrays.asList(_shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER));

    List<String> shardsToRemove = CollectionUtil.getListOfRemoved(localShards, shardsToServe);
    for (String shard : shardsToRemove) {
      File localShard = getLocalShardFolder(shard);
      LOG.info("delete local shard " + localShard.getAbsolutePath());
      FileUtil.deleteFolder(localShard);
    }
  }

  /*
   * Writes node ephemeral data into zookeeper
   */
  private void announceNode(NodeState nodeState) throws KattaException {
    LOG.info("announce node '" + _nodeName + "'...");
    final NodeMetaData metaData = new NodeMetaData(_nodeName, nodeState);
    final String nodePath = ZkPathes.getNodePath(_nodeName);
    if (_zkClient.exists(nodePath)) {
      LOG.warn("old node path '" + nodePath + "' for this node detected, delete it...");
      _zkClient.delete(nodePath);
    }

    final String nodeToShardPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    if (!_zkClient.exists(nodeToShardPath)) {
      _zkClient.create(nodeToShardPath);
    }
    _zkClient.createEphemeral(nodePath, metaData);
    LOG.info("announced node " + _nodeName);
  }

  private void startShardServing(boolean restart) throws KattaException {
    LOG.info("start serving shards...");
    final String nodeToShardPath = ZkPathes.getNode2ShardRootPath(_nodeName);
    synchronized (_zkClient.getSyncMutex()) {
      List<String> shards = _zkClient.subscribeChildChanges(nodeToShardPath, new ShardListener());
      if (restart) {
        List<String> removed = CollectionUtil.getListOfRemoved(_deployedShards, shards);
        undeployShards(removed);
      }
      deployShards(shards);
      _deployedShards.clear();
      _deployedShards.addAll(shards);
    }
  }

  protected void deployShards(final List<String> shardsToAdd) throws KattaException {
    for (String shard : shardsToAdd) {
      File localShardFolder = getLocalShardFolder(shard);
      try {
        if (!localShardFolder.exists()) {
          installShard(shard, localShardFolder);
        }
        serveShard(shard, localShardFolder);
        announceShard(shard);
      } catch (Exception e) {
        LOG.error(_nodeName + ": could not deploy shard '" + shard + "'", e);
        ShardError shardError = new ShardError(e.getMessage());
        _zkClient.createEphemeral(ZkPathes.getShard2ErrorPath(shard, _nodeName), shardError);
        FileUtil.deleteFolder(localShardFolder);
      }
    }
  }

  protected void undeployShards(final List<String> shardsToRemove) {
    for (String shard : shardsToRemove) {
      try {
        LOG.info("Undeploying shard: " + shard);
        _searcher.removeShard(shard);
        String shard2NodePath = ZkPathes.getShard2NodePath(shard, _nodeName);
        if (_zkClient.exists(shard2NodePath)) {
          _zkClient.delete(shard2NodePath);
        }
        synchronized (_shardsFolder) {
          FileUtil.deleteFolder(_shardsFolder);
        }
      } catch (final Exception e) {
        LOG.error("Failed to undeploy shard: " + shard, e);
      }
    }
  }

  /*
   * Creates an index search and adds it to the KattaMultiSearch
   */
  private void serveShard(final String shardName, final File localShardFolder) throws CorruptIndexException,
      IOException {
    IndexSearcher indexSearcher = new IndexSearcher(localShardFolder.getAbsolutePath());
    _searcher.addShard(shardName, indexSearcher);
  }

  /*
   * Announce in zookeeper node is serving this shard,
   */
  private void announceShard(String shard) throws KattaException {
    LOG.info("announce shard '" + shard + "'");
    // announce that this node serves this shard now...
    final String shard2NodePath = ZkPathes.getShard2NodePath(shard, _nodeName);
    if (_zkClient.exists(shard2NodePath)) {
      LOG.warn("detected old shard-to-node entry - deleting it..");
      // must be an old ephemeral
      _zkClient.delete(shard2NodePath);
    }

    DeployedShard deployedShard = new DeployedShard(shard, _searcher.getNumDoc(shard));
    _zkClient.createEphemeral(shard2NodePath, deployedShard);
  }

  /*
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content.
   */
  private void installShard(String shardName, File localShardFolder) throws KattaException {
    final String shardPath = readAssignedShard(shardName).getShardPath();
    LOG.info("install shard '" + shardName + "' from " + shardPath);
    URI uri;
    try {
      uri = new URI(shardPath);
      final FileSystem fileSystem = FileSystem.get(uri, new Configuration());
      final Path path = new Path(shardPath);
      boolean isZip = fileSystem.isFile(path) && shardPath.endsWith(".zip");

      File shardTmpFolder = new File(localShardFolder.getAbsolutePath() + "_tmp");
      // we download extract first to tmp dir in case something went wrong
      synchronized (_shardsFolder) {
        FileUtil.deleteFolder(localShardFolder);
        FileUtil.deleteFolder(shardTmpFolder);

        if (isZip) {
          final File shardZipLocal = new File(_shardsFolder, shardName + ".zip");
          if (shardZipLocal.exists()) {
            // make sure we overwrite cleanly
            shardZipLocal.delete();
          }
          fileSystem.copyToLocalFile(path, new Path(shardZipLocal.getAbsolutePath()));
          FileUtil.unzip(shardZipLocal, shardTmpFolder);
          shardZipLocal.delete();
        } else {
          fileSystem.copyToLocalFile(path, new Path(shardTmpFolder.getAbsolutePath()));
        }
        shardTmpFolder.renameTo(localShardFolder);
      }
    } catch (final URISyntaxException e) {
      throw new KattaException("Can not parse uri for path: " + shardPath, e);
    } catch (final IOException e) {
      throw new KattaException("Can not load shard: " + shardPath, e);
    }
  }

  public void shutdown() {
    LOG.info("shutdown " + _nodeName + " ...");
    synchronized (_zkClient.getSyncMutex()) {
      try {
        // we deleting the ephemeral's since this is the fastest and the safest
        // way, but if this does not work, it shouldn't be too bad
        _zkClient.delete(ZkPathes.getNodePath(_nodeName));
        Set<String> deployedShards = _deployedShards;
        for (String shard : deployedShards) {
          String shard2NodePath = ZkPathes.getShard2NodePath(shard, _nodeName);
          String shard2ErrorPath = ZkPathes.getShard2ErrorPath(shard, _nodeName);
          _zkClient.deleteIfExists(shard2NodePath);
          _zkClient.deleteIfExists(shard2ErrorPath);
        }
      } catch (Exception e) {
        LOG.warn("could'nt cleanup zk ephemeral pathes:" + e.getMessage());
      }
      _timer.cancel();
      _zkClient.unsubscribeAll();
      _zkClient.close();
      _rpcServer.stop();
    }
    LOG.info("shutdown " + _nodeName + " finished");
  }

  public String getName() {
    return _nodeName;
  }

  public int getSearchServerPort() {
    return _searchServerPort;
  }

  public NodeState getState() {
    return _currentState;
  }

  public void join() throws InterruptedException {
    _rpcServer.join();
  }

  public Collection<String> getDeployedShards() {
    return _deployedShards;
  }

  /*
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of node.server.port.start + 10000
   */
  private String startRPCServer(final NodeConfiguration configuration) {
    int serverPort = configuration.getStartPort();
    final String hostName = NetworkUtil.getLocalhostName();
    int tryCount = 10000;
    while (_rpcServer == null) {
      try {
        LOG.info("starting RPC server on : " + hostName);
        _rpcServer = RPC.getServer(this, "0.0.0.0", serverPort, new Configuration());
        _searchServerPort = serverPort;
      } catch (final BindException e) {
        if (configuration.getStartPort() - serverPort < tryCount) {
          serverPort++;
          // try again
        } else {
          throw new RuntimeException("tried " + tryCount + " ports and no one is free...");
        }
      } catch (final IOException e) {
        throw new RuntimeException("unable to create rpc search server", e);
      }
    }
    _searcher = new KattaMultiSearcher(_nodeName);
    try {
      _rpcServer.start();
    } catch (final IOException e) {
      throw new RuntimeException("failed to start rpc search server", e);
    }
    return hostName + ":" + serverPort;
  }

  private File getLocalShardFolder(final String shardName) {
    return new File(_shardsFolder, shardName);
  }

  /*
   * Reads AssignedShard data from ZooKeeper
   */
  private AssignedShard readAssignedShard(final String shardName) throws KattaException {
    final AssignedShard assignedShard = new AssignedShard();
    _zkClient.readData(ZkPathes.getNode2ShardPath(_nodeName, shardName), assignedShard);
    return assignedShard;
  }

  public HitsMapWritable search(final IQuery query, final DocumentFrequenceWritable freqs, final String[] shards)
      throws IOException {
    return search(query, freqs, shards, Integer.MAX_VALUE - 1);
  }

  public HitsMapWritable search(final IQuery query, final DocumentFrequenceWritable freqs, final String[] shards,
      final int count) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("You are searching with the query: '" + query.getQuery() + "'");
    }

    Query luceneQuery;
    try {
      luceneQuery = _luceneQueryParser.parse(query.getQuery());
    } catch (final ParseException e) {

      final String msg = "Failed to parse query: " + query.getQuery();
      LOG.error(msg, e);
      final IOException exception = new IOException(msg);
      exception.setStackTrace(e.getStackTrace());
      throw exception;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Lucene query: " + luceneQuery.toString());
    }

    long completeSearchTime = 0;
    final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(_nodeName);
    if (_searcher != null) {
      long start = 0;
      if (LOG.isDebugEnabled()) {
        start = System.currentTimeMillis();
      }
      _searcher.search(luceneQuery, freqs, shards, result, count);
      if (LOG.isDebugEnabled()) {
        final long end = System.currentTimeMillis();
        LOG.debug("Search took " + (end - start) / 1000.0 + "sec.");
        completeSearchTime += (end - start);
      }
    } else {
      LOG.error("No searcher for index found on '" + _nodeName + "'.");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Complete search took " + completeSearchTime / 1000.0 + "sec.");
      final DataOutputBuffer buffer = new DataOutputBuffer();
      result.write(buffer);
      LOG.debug("Result size to transfer: " + buffer.getLength());
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
      LOG.error(msg, e);
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

  private synchronized void updateStatus(NodeState state) throws KattaException {
    _currentState = state;
    final String nodePath = ZkPathes.getNodePath(_nodeName);
    final NodeMetaData metaData = new NodeMetaData();
    _zkClient.readData(nodePath, metaData);
    metaData.setState(state);
    _zkClient.writeData(nodePath, metaData);
  }

  /*
   * Listens to events within the nodeToShard zookeeper folder. Those events are
   * fired if a shard is assigned or removed for this node.
   */
  protected class ShardListener implements IZkChildListener {

    public void handleChildChange(String parentPath, List<String> shardsToServe) throws KattaException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Add/Remove shard.");
      }
      try {
        final List<String> shardsToUndeploy = CollectionUtil.getListOfRemoved(_deployedShards, shardsToServe);
        final List<String> shardsToDeploy = CollectionUtil.getListOfAdded(_deployedShards, shardsToServe);
        _deployedShards.removeAll(shardsToUndeploy);
        _deployedShards.addAll(shardsToDeploy);
        undeployShards(shardsToUndeploy);
        deployShards(shardsToDeploy);
      } finally {
        _zkClient.getSyncMutex().notifyAll();
      }
    }

  }

  /*
   * A Thread that updates the status of the node within zookeeper.
   */
  protected class StatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_nodeName != null) {
        // not yet started
        return;
      }
      long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
      time = Math.max(time, 1);
      final float qpm = (float) _queryCounter / time;
      final NodeMetaData metaData = new NodeMetaData();
      final String nodePath = ZkPathes.getNodePath(_nodeName);
      try {
        if (_zkClient.exists(nodePath)) {
          _zkClient.readData(nodePath, metaData);
          metaData.setQueriesPerMinute(qpm);
          _zkClient.writeData(nodePath, metaData);
        }
      } catch (final Exception e) {
        LOG.error("Failed to update node status.", e);
      }
    }
  }

}
