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

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.util.ComparisonUtil;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

public class Node implements ISearch, IZkReconnectListener {

	protected final static Logger LOG = Logger.getLogger(Node.class);

	public static final long _protocolVersion = 0;
	@SuppressWarnings("unused")
	private static final long versionID = 0;

	protected ZKClient _zkClient;
	private Server _rpcServer;
	private KattaMultiSearcher _searcher;

	private final QueryParser _luceneQueryParser = new QueryParser("field",
			new KeywordAnalyzer());

	protected String _nodeName;
	protected File _shardsFolder;
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

		LOG.debug("Starting rpc server...");
		_nodeName = startRPCServer(_configuration);

		LOG.debug("Starting zk client...");
		if (!_zkClient.isStarted()) {
			_zkClient.start(30000);
		}
		ArrayList<String> shards = announceNode();
		updateStatus(NodeState.STARTING);
		_searcher = new KattaMultiSearcher(_nodeName);
		checkAndDeployExistingShards(NodeState.STARTING, shards);

		LOG.info("Started: " + _nodeName + "...");

		if (_rpcServer != null) {
			try {
				_rpcServer.start();
			} catch (final IOException e) {
				throw new RuntimeException("Failed to start node server.", e);
			}
		} else {
			throw new RuntimeException(
					"tried 10000 ports and no one is free...");
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
		return _nodeName;
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
	private ArrayList<String> announceNode() throws KattaException {
		LOG.info("Announces node " + _nodeName);
		final NodeMetaData metaData = new NodeMetaData(_nodeName,
				NodeState.STARTING.name(), true, _startTime);
		final String nodePath = ZkPathes.getNodePath(_nodeName);
		if (_zkClient.exists(nodePath)) {
			LOG.warn("Old node path '" + nodePath
					+ "' for this node detected, delete it...");
			_zkClient.delete(nodePath);
		}
		_zkClient.createEphemeral(nodePath, metaData);

		final String nodeToShardPath = ZkPathes
				.getNode2ShardRootPath(_nodeName);
		if (!_zkClient.exists(nodeToShardPath)) {
			_zkClient.create(nodeToShardPath);
		}
		LOG.debug("Add shard listener in node.");
		return _zkClient.subscribeChildChanges(nodeToShardPath,
				new ShardListener());
	}

	public void handleReconnect() throws KattaException {
		ArrayList<String> shardsToServe = announceNode();
		updateStatus(NodeState.RECONNECTING);
		LOG.info("My old shards to serve: " + shardsToServe);
		checkAndDeployExistingShards(NodeState.RECONNECTING, shardsToServe);
		updateStatus(NodeState.STARTING);
	}

	/*
	 * Starting the hadoop RPC server that response to query requests. We
	 * iterate over a port range of node.server.port.start + 10000
	 */
	private String startRPCServer(final NodeConfiguration configuration) {
		int serverPort = configuration.getStartPort();
		final String hostName = NetworkUtil.getLocalhostName();
		for (int i = serverPort; i < (serverPort + 10000); i++) {
			try {
				LOG.info("starting RPC server on : " + hostName);
				_rpcServer = RPC.getServer(this, "0.0.0.0", i,
						new Configuration());
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
	 * When starting a node there might be shards assigned that are still on
	 * local hhd. Therefore we compare what is assigned, what can be re-used and
	 * which shards we need to load from the remote hdd.
	 */
	private void checkAndDeployExistingShards(NodeState nodeState,
			final ArrayList<String> shardsToDeploy) throws KattaException {
		synchronized (_shardsFolder) {
			List<String> localShards = Arrays.asList(_shardsFolder
					.list(new FilenameFilter() {
						public boolean accept(final File dir, final String name) {
							return !name.startsWith(".");
						}
					}));

			// remove exiting but not to deploy
			List<String> removedShards = ComparisonUtil.getRemoved(localShards,
					shardsToDeploy);
			removeShards(removedShards);

			// now only download those we do not yet have local
			final HashSet<String> existingShards = new HashSet<String>();
			existingShards.addAll(localShards);

			for (final String shard : shardsToDeploy) {
				DeployedShard deployedShard = null;
				final AssignedShard assignedShard = getAssignedShard(shard);
				File shardTargetFolder = new File(_shardsFolder, shard);
				try {
					// load first or use local file
					if (!existingShards.contains(shard)) {
						LOG.debug("Shard '" + shard + "' has to be deployed.");
						loadAndUnzipShard(nodeState, assignedShard,
								shardTargetFolder);
					}

					// deploy and announce
					final int numOfDocs = deployShard(shard, shardTargetFolder);
					deployedShard = new DeployedShard(shard, System
							.currentTimeMillis(), numOfDocs);
				} catch (final Exception e) {
					FileUtil.deleteFolder(shardTargetFolder);
					updateStatusWithError(e);
					LOG.error("Unable to load shard:", e);
					deployedShard = new DeployedShard(shard, System
							.currentTimeMillis(), 0);
					deployedShard.setErrorMsg(e.getMessage());
				} finally {
					announceShard(deployedShard);
				}
			}
		}
	}

	protected void deployAndAnnounceShards(NodeState state,
			final List<String> shardsToServe) {
		for (final String shardName : shardsToServe) {
			deployAndAnnounceShard(state, shardName);
		}
	}

	/*
	 * Loads, deploys and announce a single fresh assigned shard.
	 */
	private void deployAndAnnounceShard(NodeState state, final String shardName) {
		DeployedShard deployedShard = null;
		File shardTargetFolder = new File(_shardsFolder, shardName);
		AssignedShard assignedShard = null;
		try {
			assignedShard = getAssignedShard(shardName);
			loadAndUnzipShard(state, assignedShard, shardTargetFolder);
			final int numOfDocs = deployShard(shardName, shardTargetFolder);
			deployedShard = new DeployedShard(shardName, System
					.currentTimeMillis(), numOfDocs);
		} catch (final Exception e) {
			if (shardTargetFolder.exists()) {
				FileUtil.deleteFolder(shardTargetFolder);
			}
			LOG.error("Unable to load shard:", e);
			updateStatusWithError(e);
			deployedShard = new DeployedShard(shardName, System
					.currentTimeMillis(), 0);
			deployedShard.setErrorMsg(e.getMessage());
		} finally {
			announceShard(deployedShard);
		}
	}

	/*
	 * Loads a shard from the given URI. The uri is handled bye the hadoop file
	 * system. So all hadoop support file systems can be used, like local hdfs
	 * s3 etc. In case the shard is compressed we also unzip the content.
	 */
	private void loadAndUnzipShard(NodeState nodeState,
			final AssignedShard assignedShard, File shardTargetFolder) {
		final String shardKey = assignedShard.getShardName();
		final String shardPath = assignedShard.getShardPath();
		URI uri;
		try {
			uri = new URI(shardPath);
			final FileSystem fileSystem = FileSystem.get(uri,
					new Configuration());
			final Path path = new Path(shardPath);
			boolean isZip = fileSystem.isFile(path)
					&& shardPath.endsWith(".zip");

			File shardTmpFolder = new File(shardTargetFolder.getAbsolutePath()
					+ "_tmp");
			// we download extract first to tmp dir in case something went wrong
			synchronized (_shardsFolder) {
				FileUtil.deleteFolder(shardTargetFolder);
				FileUtil.deleteFolder(shardTmpFolder);

				if (isZip) {
					final File shardZipLocal = new File(_shardsFolder,
							assignedShard.getShardName() + ".zip");
					updateStatus(nodeState, "copy shard: " + shardKey);
					if (shardZipLocal.exists()) {
						// make sure we overwrite cleanly
						shardZipLocal.delete();
					}
					fileSystem.copyToLocalFile(path, new Path(shardZipLocal
							.getAbsolutePath()));

					// decompress
					updateStatus(nodeState, "decompressing shard: " + shardKey);
					FileUtil.unzip(shardZipLocal, shardTmpFolder);

					// remove zip
					shardZipLocal.delete();
				} else {
					updateStatus(nodeState, "copy shard: " + shardKey);
					fileSystem.copyToLocalFile(path, new Path(shardTmpFolder
							.getAbsolutePath()));
				}
				shardTmpFolder.renameTo(shardTargetFolder);
				updateStatus(nodeState, "Load and Unzip Shard ready.");
			}
		} catch (final URISyntaxException e) {
			final String msg = "Can not parse uri for path: "
					+ assignedShard.getShardPath();
			LOG.error(msg, e);
			updateStatusWithError(msg);
			updateShardStatusInNode(assignedShard.getIndexName(), msg);
			throw new RuntimeException(msg, e);
		} catch (final IOException e) {
			final String msg = "Can not load shard: "
					+ assignedShard.getShardPath();
			LOG.error(msg, e);
			updateStatusWithError(msg);
			updateShardStatusInNode(assignedShard.getIndexName(), msg);
			throw new RuntimeException(msg, e);
		}
	}

	/*
	 * Creates an index search and adds it to the KattaMultiSearch
	 */
	private int deployShard(final String shardName, final File localShardFolder) {
		IndexSearcher indexSearcher;
		try {
			indexSearcher = new IndexSearcher(localShardFolder
					.getAbsolutePath());
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
	private AssignedShard getAssignedShard(final String shardName)
			throws KattaException {
		final AssignedShard assignedShard = new AssignedShard();
		_zkClient.readData(ZkPathes.getNode2ShardPath(_nodeName, shardName),
				assignedShard);
		return assignedShard;
	}

	/*
	 * Announce in zookeeper node is serving this shard,
	 */

	private void announceShard(final DeployedShard deployedShard) {
		try {
			// announce that this node serves this shard now...
			final String nodePath = ZkPathes.getShard2NodePath(deployedShard
					.getShardName(), _nodeName);
			if (!_zkClient.exists(nodePath)) {
				_zkClient.createEphemeral(nodePath, deployedShard);
			} else {
				// only update
				_zkClient.writeData(nodePath, deployedShard);
			}
		} catch (final Exception e) {
			// TODO jz: update status ??
			LOG.error("Unable to serve Shard: " + deployedShard, e);
		}
	}

	protected void removeShards(final List<String> shardsToRemove) {
		LOG.info("Removing shards: " + shardsToRemove);
		for (final String shardName : shardsToRemove) {
			removeShard(shardName);
		}
	}

	/*
	 * Removes the shard from KattaMultiSearcher, our list of deployed Shards
	 * and removes the index from local hdd.
	 */
	private void removeShard(final String shardName) {
		try {
			LOG.info("Removing shard: " + shardName);
			_searcher.removeShard(shardName);
			_deployedShards.remove(shardName);
			String shard2NodePath = ZkPathes.getShard2NodePath(shardName,
					_nodeName);
			if (_zkClient.exists(shard2NodePath)) {
				// remove node serving it.
				// TODO shouldn't that be done by the master ?
				_zkClient.delete(shard2NodePath);
			}

			final String shard2NodeRootPath = ZkPathes
					.getShard2NodeRootPath(shardName);
			if (_zkClient.exists(shard2NodeRootPath)
					&& _zkClient.getChildren(shard2NodeRootPath).size() == 0) {
				// this was the last node
				// TODO shouldn't that be done by the master ?
				_zkClient.delete(shard2NodeRootPath);
			}
			synchronized (_shardsFolder) {
				FileUtil.deleteFolder(_shardsFolder);
			}
		} catch (final Exception e) {
			LOG.error("Failed to remove local shard: " + shardName, e);
		}
	}

	public HitsMapWritable search(final IQuery query,
			final DocumentFrequenceWritable freqs, final String[] shards)
			throws IOException {
		return search(query, freqs, shards, Integer.MAX_VALUE - 1);
	}

	public HitsMapWritable search(final IQuery query,
			final DocumentFrequenceWritable freqs, final String[] shards,
			final int count) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("You are searching with the query: '" + query.getQuery()
					+ "'");
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
		final HitsMapWritable result = new net.sf.katta.node.HitsMapWritable(
				_nodeName);
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
			LOG.debug("Complete search took " + completeSearchTime / 1000.0
					+ "sec.");
			final DataOutputBuffer buffer = new DataOutputBuffer();
			result.write(buffer);
			LOG.debug("Result size to transfer: " + buffer.getLength());
		}
		return result;
	}

	public long getProtocolVersion(final String protocol,
			final long clientVersion) throws IOException {
		return _protocolVersion;
	}

	public DocumentFrequenceWritable getDocFreqs(final IQuery input,
			final String[] shards) throws IOException {
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
	public MapWritable getDetails(final String shard, final int docId)
			throws IOException {
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

	public MapWritable getDetails(final String shard, final int docId,
			final String[] fieldNames) throws IOException {
		final MapWritable result = new MapWritable();
		final Document doc = _searcher.doc(shard, docId);
		for (final String fieldName : fieldNames) {
			final Field field = doc.getField(fieldName);
			if (field != null) {
				if (field.isBinary()) {
					final byte[] binaryValue = field.binaryValue();
					result.put(new Text(fieldName), new BytesWritable(
							binaryValue));
				} else {
					final String stringValue = field.stringValue();
					result.put(new Text(fieldName), new Text(stringValue));
				}
			}
		}
		return result;
	}

	public int getResultCount(final IQuery query, final String[] shards)
			throws IOException {
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

	private synchronized void updateStatus(NodeState state,
			final String statusMsg) {
		_currentState = state;
		final String nodePath = ZkPathes.getNodePath(_nodeName);
		final NodeMetaData metaData = new NodeMetaData();
		try {
			_zkClient.readData(nodePath, metaData);
			if (statusMsg == null) {
				metaData.setStatus(state.name());
			} else {
				metaData.setStatus(state + ": " + statusMsg);
			}
			metaData.setStarting(state == NodeState.STARTING);
			_zkClient.writeData(nodePath, metaData);
		} catch (KattaException e) {
			LOG.error("Cannot update node status.", e);
		}
	}

	private void updateStatusWithError(Exception exception) {
		// TODO jz: stringify exception / multiple exceptions ?
		updateStatusWithError(exception.getMessage());
	}

	private void updateStatusWithError(String errorString) {
		final String nodePath = ZkPathes.getNodePath(_nodeName);
		final NodeMetaData metaData = new NodeMetaData();
		try {
			_zkClient.readData(nodePath, metaData);
			metaData.setException(errorString);
			_zkClient.writeData(nodePath, metaData);
		} catch (KattaException e) {
			LOG.error("Cannot update node status.", e);
		}
	}

	private void updateShardStatusInNode(final String shardName,
			final String errorMsg) {
		final String shard2nodePath = ZkPathes.getShard2NodePath(shardName,
				_nodeName);
		DeployedShard deployedShard = new DeployedShard();
		try {
			if (_zkClient.exists(shard2nodePath)) {
				_zkClient.readData(shard2nodePath, deployedShard);
				if (null == errorMsg) {
					deployedShard.cleanError();
				} else {
					deployedShard.setErrorMsg(errorMsg);
				}
				_zkClient.writeData(shard2nodePath, deployedShard);
			}
		} catch (final Exception e) {
			LOG.error("Failed to write shard status." + deployedShard, e);
		}
	}

	/*
	 * Listens to events within the nodeToShard zookeeper folder. Those events
	 * are fired if a shard is assigned or removed for this node.
	 */
	protected class ShardListener implements IZkChildListener {

		public void handleChildChange(String parentPath) throws KattaException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Add/Remove shard.");
			}
			synchronized (_zkClient.getSyncMutex()) {
				try {
					List<String> shardsToServe = _zkClient
							.getChildren(parentPath);
					final List<String> shardsToRemove = ComparisonUtil
							.getRemoved(_deployedShards, shardsToServe);
					removeShards(shardsToRemove);
					final List<String> shardsToDeploy = ComparisonUtil.getNew(
							_deployedShards, shardsToServe);
					deployAndAnnounceShards(NodeState.IN_SERVICE,
							shardsToDeploy);
				} catch (final KattaException e) {
					throw new RuntimeException(
							"Failed to execute redployment instruction", e);
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
			if (_nodeName != null) {
				long time = (System.currentTimeMillis() - _startTime)
						/ (60 * 1000);
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
				} catch (final KattaException e) {
					LOG.error("Failed to update node status (StatusUpdater).",
							e);
				}
			}
		}
	}

	public NodeState getState() {
		return _currentState;
	}

}
