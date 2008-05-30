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
package net.sf.katta.slave;

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
import net.sf.katta.util.SlaveConfiguration;
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

public class Slave implements ISearch {

  public static final long _protocolVersion = 0;

  private static final long versionID = 0;

  private static final int BUFFER = 4096;

  private Server _server;

  private final QueryParser _luceneQueryParser = new QueryParser("field", new KeywordAnalyzer());

  private KattaMultiSearcher _searcher;

  ZKClient _client;

  private final ArrayList<String> _deployed = new ArrayList<String>();

  private File _shardFolder;

  private String _slave;

  private final long _startTime;

  private long _queryCounter;

  private final Timer _timer;

  public Slave(final ZKClient client) {
    _client = client;
    _startTime = System.currentTimeMillis();
    _timer = new Timer("QueryCounter", true);
    _timer.schedule(new StatusUpdater(), new Date(), 60 * 1000);
  }

  /**
   * Boots the slave
   * 
   * @throws KattaException
   */

  public void start() throws KattaException {
    Logger.debug("Starting slave...");
    _client.waitForZooKeeper(30000);
    _client.createDefaultNameSpace();
    final SlaveConfiguration configuration = new SlaveConfiguration();

    final String shardFolder = configuration.getShardFolder();
    _shardFolder = new File(shardFolder);
    if (!_shardFolder.exists()) {
      _shardFolder.mkdirs();
    }
    _slave = startRPCServer(configuration);
    final ArrayList<String> shardsToServe = announceSlave();

    updateStatus("STARTING");
    _searcher = new KattaMultiSearcher(_slave);
    checkAndDeployExistingShards(shardsToServe);
    Logger.info("Started: " + _slave + "...");

    if (_server != null) {
      try {
        _server.start();
      } catch (final IOException e) {
        throw new RuntimeException("Failed to start slave server.", e);
      }
    } else {
      throw new RuntimeException("tried 10000 ports and no one is free...");
    }
    updateStatus("OK");
  }

  public void join() {
    try {
      _server.join();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Failed to join slave", e);
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

  /*
   * Writes slave ephemeral data into zookeeper
   */
  private ArrayList<String> announceSlave() throws KattaException {
    Logger.debug("Announces slave " + _slave);
    final SlaveMetaData metaData = new SlaveMetaData(_slave, "booting", true, _startTime);
    final String slavePath = IPaths.SLAVES + "/" + _slave;
    final String slaveToShardPath = IPaths.SLAVE_TO_SHARD + "/" + _slave;
    _client.createEphemeral(slavePath, metaData);
    if (!_client.exists(slaveToShardPath)) {
      _client.create(slaveToShardPath);
    }
    return _client.subscribeChildChanges(slaveToShardPath, new ShardListener());
  }

  /*
   * Starting the hadoop RPC server that response to query requests. We iterate
   * over a port range of slave.server.port.start + 10000
   */
  private String startRPCServer(final SlaveConfiguration configuration) {
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
        throw new RuntimeException("unable to start slave server", e);
      }
    }
    return hostName + ":" + serverPort;
  }

  /*
   * When starting a slave there might be shards assigned that are still on
   * local hhd. Therefore we compare what is assigned, what can be re-used and
   * which shards we need to load from the remote hdd.
   */
  private void checkAndDeployExistingShards(final ArrayList<String> shardsToDeploy) throws KattaException {
    synchronized (_shardFolder) {
      final String[] localShards = _shardFolder.list(new FilenameFilter() {
        public boolean accept(final File dir, final String name) {
          return !name.startsWith(".");
        }
      });
      // in case not shards are assigned we want to remove local shards.
      if (shardsToDeploy == null || shardsToDeploy.size() == 0) {
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
          throw new KattaException("Faild use local hadoop file system.", e);
        }
      }
      // remove those we do not need anymore
      final List<String> localShardList = Arrays.asList(localShards);
      final List<String> toRemove = ComparisonUtil.getRemoved(localShardList, shardsToDeploy);
      // removeShards(toRemove);

      // now only download those we do not yet have local or we can't deploy
      if (shardsToDeploy != null && shardsToDeploy.size() != 0) {
        final HashSet<String> existingShards = new HashSet<String>();
        existingShards.addAll(localShardList);

        for (final String shardName : shardsToDeploy) {
          final AssignedShard assignedShard = getAssignedShard(shardName);

          File localShardFolder = null;
          try {

            // load first or use local file
            if (!existingShards.contains(shardName)) {
              localShardFolder = loadAndUnzipShard(assignedShard);
            } else {
              localShardFolder = new File(_shardFolder, shardName);
            }
            // deploy and announce
            final int numOfDocs = deployShard(shardName, localShardFolder);
            final DeployedShard deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), numOfDocs);
            announceShard(deployedShard);

          } catch (final Exception e) {
            if (localShardFolder != null) {
              if (localShardFolder.exists()) {
                deleteFolder(localShardFolder);
              }
            }
            if (Logger.isError()) {
              Logger.error("Unable to load shard:", e);
            }
          }
        }
      }
    }
  }

  private void deployAndAnnounceShards(final List<String> shardsToServe) {
    for (final String shardName : shardsToServe) {
      deployAndAnnoucneShard(shardName);
    }
  }

  /*
   * Loads, deploys and announce a single fresh assigned shard.
   */
  private void deployAndAnnoucneShard(final String shardName) {
    File localShardFolder = null;
    try {
      final AssignedShard assignedShard = getAssignedShard(shardName);
      localShardFolder = loadAndUnzipShard(assignedShard);
      final int numOfDocs = deployShard(shardName, localShardFolder);
      final DeployedShard deployedShard = new DeployedShard(shardName, System.currentTimeMillis(), numOfDocs);
      announceShard(deployedShard);
    } catch (final Exception e) {
      if (localShardFolder != null) {
        if (localShardFolder.exists()) {
          deleteFolder(localShardFolder);
        }
      }
      if (Logger.isError()) {
        Logger.error("Unable to load shard:", e);
      }
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
        return shardFolder;
      }
    } catch (final URISyntaxException e) {
      final String msg = "Can not parse uri for path: " + assignedShard.getShardPath();
      Logger.error(msg, e);
      throw new RuntimeException(msg, e);
    } catch (final IOException e) {
      final String msg = "Can not load shard: " + assignedShard.getShardPath();
      Logger.error(msg, e);
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

        Logger.debug("Extracting: " + entry);
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
      return indexSearcher.maxDoc();
    } catch (final Exception e) {
      throw new RuntimeException("Shard index " + shardName + " can't be started", e);
    }
  }

  /*
   * Reads AssignedShard data from ZooKeeper
   */
  private AssignedShard getAssignedShard(final String shardName) throws KattaException {
    final AssignedShard assignedShard = new AssignedShard();
    _client.readData(IPaths.SLAVE_TO_SHARD + "/" + _slave + "/" + shardName, assignedShard);
    return assignedShard;
  }

  /*
   * Announce in zookeeper slave is serving this shard,
   */

  private void announceShard(final DeployedShard deployedShard) {
    try {
      final String shardPath = IPaths.SHARD_TO_SLAVE + "/" + deployedShard.getShardName();
      // announce that this slave serves this shard now...
      final String slavePath = shardPath + "/" + _slave;
      if (!_client.exists(slavePath)) {
        _client.createEphemeral(slavePath, deployedShard);
      } else {
        _client.writeData(slavePath, deployedShard);
      }
    } catch (final Exception e) {
      if (Logger.isError()) {
        Logger.error("Unable to server Shard: " + deployedShard, e);
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
      _deployed.remove(shardName);
      final String shardPath = IPaths.SHARD_TO_SLAVE + "/" + shardName;
      final String slavePath = shardPath + "/" + _slave;
      if (_client.exists(slavePath)) {
        _client.delete(slavePath);
      } // remove slave serving it.
      if (_client.getChildren(shardPath).size() == 0) {
        // this was the last slave
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
   * @see net.sf.katta.slave.ISearch#search(net.sf.katta.slave.IQuery,
   *      net.sf.katta.slave.DocumentFrequenceWritable, java.lang.String[])
   */
  public HitsMapWritable search(final IQuery query, final DocumentFrequenceWritable freqs, final String[] shards)
  throws IOException {
    return search(query, freqs, shards, Integer.MAX_VALUE - 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.slave.ISearch#search(net.sf.katta.slave.IQuery,
   *      net.sf.katta.slave.DocumentFrequenceWritable, java.lang.String[], int)
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
    final HitsMapWritable result = new net.sf.katta.slave.HitsMapWritable(_slave);
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
      Logger.error("No searcher for index found on '" + _slave + "'.");
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
   * @see net.sf.katta.slave.ISearch#getDocFreqs(net.sf.katta.slave.IQuery,
   *      java.lang.String[])
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
   * @see net.sf.katta.slave.ISearch#getDetails(java.lang.String, int)
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
   * @see net.sf.katta.slave.ISearch#getDetails(java.lang.String, int,
   *      java.lang.String[])
   */
  public MapWritable getDetails(final String shard, final int docId, final String[] fieldNames) throws IOException {
    final MapWritable result = new MapWritable();
    final Document doc = _searcher.doc(shard, docId);
    for (final String fieldName : fieldNames) {
      final Field field = doc.getField(fieldName);
      if (field.isBinary()) {
        final byte[] binaryValue = field.binaryValue();
        result.put(new Text(fieldName), new BytesWritable(binaryValue));
      } else {
        final String stringValue = field.stringValue();
        result.put(new Text(fieldName), new Text(stringValue));
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see net.sf.katta.slave.ISearch#getResultCount(net.sf.katta.slave.IQuery,
   *      java.lang.String[])
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
   * Updates the status of the slave in zookeeper.
   */
  private void updateStatus(final String statusMsg) throws KattaException {
    final String path = IPaths.SLAVES + "/" + _slave;
    final SlaveMetaData metaData = new SlaveMetaData();
    _client.readData(path, metaData);
    metaData.setStatus(statusMsg);
    _client.writeData(path, metaData);
  }

  /*
   * Listens to events within the slaveToShard zookeeper folder. Those events
   * are fired if a shard is assigned or removed for this slave.
   */
  private class ShardListener implements IZKEventListener {
    public void process(final WatcherEvent event) {
      synchronized (_client.getSyncMutex()) {
        if (Logger.isDebug()) {
          Logger.debug("ShardListener.process()" + _slave);
        }
        final String path = event.getPath();
        List<String> newList;
        try {
          newList = _client.getChildren(path);
          final List<String> shardsToRemove = ComparisonUtil.getRemoved(_deployed, newList);
          removeShards(shardsToRemove);
          final List<String> shardsToServe = ComparisonUtil.getNew(_deployed, newList);
          deployAndAnnounceShards(shardsToServe);
          _client.getSyncMutex().notifyAll();
        } catch (final KattaException e) {
          throw new RuntimeException("Failed to read zookeeper information");
        }
      }
    }
  }

  /*
   * A Thread that updates the status of the slave within zookeeper.
   */
  private class StatusUpdater extends TimerTask {
    @Override
    public void run() {
      if (_slave != null) {
        long time = (System.currentTimeMillis() - _startTime) / (60 * 1000);
        time = Math.max(time, 1);
        final float qpm = (float) _queryCounter / time;
        final SlaveMetaData metaData = new SlaveMetaData();
        final String path = IPaths.SLAVES + "/" + _slave;
        try {
          if (_client.exists(path)) {
            _client.readData(path, metaData);
            metaData.setQueriesPerMinute(qpm);
            _client.writeData(path, metaData);
          }
        } catch (final KattaException e) {
          Logger.error("Failed to update data in zookeeper", e);
        }
      }
    }
  }
}
