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
import net.sf.katta.zk.IZKEventListener;
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

import com.yahoo.zookeeper.proto.WatcherEvent;

public class Node implements ISearch {

  public static final long _protocolVersion = 0;

  @SuppressWarnings("unused")
  private static final long versionID = 0;

  private static final int BUFFER = 4096;

  private Server _server;

  private final QueryParser _luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());

  private KattaMultiSearcher _searcher;

  ZKClient _client;

  private final ArrayList<String> _deployedShards = new ArrayList<String>();

  private File _shardFolder;

  private String _node;

  public String getNode() {
    return _node;
  }

  private final long _startTime;

  private long _queryCounter;

  private final Timer _timer;

  private final NodeConfiguration _configuration;

  public Node(final ZKClient client) {
    this(client, new NodeConfiguration());
  }

  public Node(final ZKClient client, final NodeConfiguration configuration) {
    _client = client;
    _configuration = configuration;
    _startTime = System.currentTimeMillis();
    _timer = new Timer("QueryCounter", true);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  /**
   * Boots the node
   * 
   * @throws KattaException
   */

  public void start() throws KattaException {
    Logger.debug("Starting node...");
    _client.waitForZooKeeper(30000);
    _client.createDefaultNameSpace();

    final String shardFolder = _configuration.getShardFolder();
    _shardFolder = new File(shardFolder);
    if (!_shardFolder.exists()) {
      _shardFolder.mkdirs();
    }
    _node = startRPCServer(_configuration);
    final ArrayList<String> shardsToServe = announceNode();
    Logger.info("My old shards to serve: " + shardsToServe);

    updateStatus("STARTING", true);
    _searcher = new KattaMultiSearcher(_node);
    checkAndDeployExistingShards(shardsToServe);
    Logger.info("Started: " + _node + "...");

    if (_server != null) {
      try {
        _server.start();
      } catch (final IOException e) {
        throw new RuntimeException("Failed to start node server.", e);
      }
    } else {
      throw new RuntimeException("tried 10000 ports and no one is free...");
    }
    updateStatus("OK", false);
  }

  public void join() {
    try {
      _server.join();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to join node", e);
    }
  }

  public void shutdown() {
    _timer.cancel();
    // TODO do we really need to close the zk here?
    if (_client != null) {
      _client.close();
    }
    _server.stop();
  }

  public ArrayList<String> getDeployShards() {
    return _deployedShards;
  }

  /*
   * Writes node ephemeral data into zookeeper
   */
  private ArrayList<String> announceNode() throws KattaException {
    Logger.debug("Announces node " + _node);
    final NodeMetaData metaData = new NodeMetaData(_node, "booting", true, _startTime);
    final String nodePath = IPaths.NODES + "/" + _node;
    final String nodeToShardPath = IPaths.NODE_TO_SHARD + "/" + _node;
    if (_client.exists(nodePath)) {
      Logger.debug("Old node for this host detected, will be removed...");
      _client.delete(nodePath);
    }
    _client.createEphemeral(nodePath, metaData);
    if (!_client.exists(nodeToShardPath)) {
      _client.create(nodeToShardPath);
    }
    Logger.debug("Add shard listener in node.");
    return _client.subscribeChildChanges(nodeToShardPath, new ShardListener());
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
        _server = RPC.getServer(this, "0.0.0.0", i, new Configuration());
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
  private void checkAndDeployExistingShards(final ArrayList<String> shardsToDeploy) throws KattaException {
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
              localShardFolder = loadAndUnzipShard(assignedShard);
            } else {
              Logger.debug("Shard '" + shardName + "' already deployed.");
              localShardFolder = new File(_shardFolder, shardName);
            }
            // deploy and announce
            final int numOfDocs = deployShard(shardName, localShardFolder);
            deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), numOfDocs);
          } catch (final Exception e) {
            updateStatus("Error: " + e.getMessage());
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

  private void deployAndAnnounceShards(final List<String> shardsToServe) {
    for (final String shardName : shardsToServe) {
      try {
        deployAndAnnounceShard(shardName);
      } catch (KattaException e) {
        Logger.error("Cannot deploy shard '" + shardName + "'.", e);
      }
    }
  }

  /*
   * Loads, deploys and announce a single fresh assigned shard.
   */
  private void deployAndAnnounceShard(final String shardName) throws KattaException {
    DeployedShard deployedShard = null;
    File localShardFolder = null;
    AssignedShard assignedShard = null;
    try {
      assignedShard = getAssignedShard(shardName);
      localShardFolder = loadAndUnzipShard(assignedShard);
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
      updateStatus("Error: " + e.getMessage());
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
  private File loadAndUnzipShard(final AssignedShard assignedShard) throws KattaException {
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
          updateStatus("copy shard: " + shardKey);
          // make sure we overwrite cleanly
          if (fileSystem.exists(dest)) {
            fileSystem.delete(dest);
          }
          fileSystem.copyToLocalFile(path, dest);
          // decompress
          updateStatus("decompressing shard: " + shardKey);
          decompress(shardZip, shardFolder);
          // remove zip
          shardZip.delete();
        } else {
          final Path dest = new Path(shardFolder.getAbsolutePath());
          // make sure we overwrite cleanly
          if (fileSystem.exists(dest)) {
            fileSystem.delete(dest);
          }
          updateStatus("copy shard: " + shardKey);
          fileSystem.copyToLocalFile(path, dest);

        }
        updateStatus("Load and Unzip Shard ready.");
        return shardFolder;
      }
    } catch (final URISyntaxException e) {
      final String msg = "Can not parse uri for path: " + assignedShard.getShardPath();
      Logger.error(msg, e);
      updateStatus("Error: " + msg);
      updateShardStatusInNode(assignedShard.getIndexName(), msg);

      throw new RuntimeException(msg, e);
    } catch (final IOException e) {
      final String msg = "Can not load shard: " + assignedShard.getShardPath();
      Logger.error(msg, e);
      updateStatus("Error: " + msg);
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
    _client.readData(IPaths.NODE_TO_SHARD + "/" + _node + "/" + shardName, assignedShard);
    return assignedShard;
  }

  /*
   * Announce in zookeeper node is serving this shard,
   */

  private void announceShard(final DeployedShard deployedShard) {
    try {
      final String shardPath = IPaths.SHARD_TO_NODE + "/" + deployedShard.getShardName();
      // announce that this node serves this shard now...
      final String nodePath = shardPath + "/" + _node;
      if (!_client.exists(nodePath)) {
        _client.createEphemeral(nodePath, deployedShard);
      } else {
        _client.writeData(nodePath, deployedShard);
      }
    } catch (final Exception e) {
      if (Logger.isError()) {
        Logger.error("Unable to serve Shard: " + deployedShard, e);
      }
    }
  }

  private void removeShards(final List<String> shardsToRemove) {
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
      final String nodePath = shardPath + "/" + _node;
      if (_client.exists(nodePath)) {
        _client.delete(nodePath);
      } // remove node serving it.
      if (_client.exists(shardPath) && _client.getChildren(shardPath).size() == 0) {
        // this was the last node
        _client.delete(shardPath);
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

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#search(net.sf.katta.node.IQuery,
   * net.sf.katta.node.DocumentFrequenceWritable, java.lang.String[], int)
   */
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
    final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(_node);
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
      Logger.error("No searcher for index found on '" + _node + "'.");
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

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#getDocFreqs(net.sf.katta.node.IQuery,
   * java.lang.String[])
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#getDetails(java.lang.String, int,
   * java.lang.String[])
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.node.ISearch#getResultCount(net.sf.katta.node.IQuery,
   * java.lang.String[])
   */
  public int getResultCount(final IQuery query, final String[] shards) throws IOException {
    final DocumentFrequenceWritable docFreqs = getDocFreqs(query, shards);
    return search(query, docFreqs, shards, 1).getTotalHits();
  }

  @Override
  protected void finalize() throws Throwable {
    shutdown();
  }

  /*
   * Updates the status of the node in zookeeper.
   */
  private void updateStatus(final String statusMsg) {
    final String path = IPaths.NODES + "/" + _node;
    final NodeMetaData metaData = new NodeMetaData();
    try {
      _client.readData(path, metaData);
      metaData.setStatus(statusMsg);
      _client.writeData(path, metaData);
    } catch (KattaException e) {
      Logger.error("Cannot update node status.", e);
    }
  }

  /*
   * Updates the status of the node in zookeeper.
   */
  private void updateStatus(final String statusMsg, final boolean starting) throws KattaException {
    final String path = IPaths.NODES + "/" + _node;
    final NodeMetaData metaData = new NodeMetaData();
    try {
      _client.readData(path, metaData);
      metaData.setStatus(statusMsg);
      metaData.setStarting(starting);
      _client.writeData(path, metaData);
    } catch (KattaException e) {
      Logger.error("Cannot update node status.", e);
    }
  }

  private void updateShardStatusInNode(final String shardName, final String errorMsg) {
    final String nodePath = IPaths.SHARD_TO_NODE + "/" + shardName + "/" + _node;
    DeployedShard deployedShard = new DeployedShard();
    try {
      if (_client.exists(nodePath)) {
        _client.readData(nodePath, deployedShard);
        if (null == errorMsg) {
          deployedShard.cleanError();
        } else {
          deployedShard.setErrorMsg(errorMsg);
        }
        _client.writeData(nodePath, deployedShard);
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
  private class ShardListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      Logger.debug("Add/Remove shard.");
      synchronized (_client.getSyncMutex()) {
        if (Logger.isDebug()) {
          Logger.debug("ShardListener.process()" + _node);
        }
        final String path = event.getPath();
        List<String> newList;
        try {
          newList = _client.getChildren(path);
          final List<String> shardsToRemove = ComparisonUtil.getRemoved(_deployedShards, newList);
          removeShards(shardsToRemove);
          final List<String> shardsToServe = ComparisonUtil.getNew(_deployedShards, newList);
          deployAndAnnounceShards(shardsToServe);
        } catch (final KattaException e) {
          throw new RuntimeException("Failed to read zookeeper information");
        } finally {
          _client.getSyncMutex().notifyAll();
        }
      }
    }
  }

  /*
   * A Thread that updates the status of the node within zookeeper.
   */
  private class StatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_node != null) {
        long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
        time = Math.max(time, 1);
        final float qpm = (float) _queryCounter / time;
        final NodeMetaData metaData = new NodeMetaData();
        final String path = IPaths.NODES + "/" + _node;
        try {
          if (_client.exists(path)) {
            _client.readData(path, metaData);
            metaData.setQueriesPerMinute(qpm);
            _client.writeData(path, metaData);
          }
        } catch (final KattaException e) {
          Logger.error("Failed to update node status (StatusUpdater).", e);
        }
      }
    }
  }
}
