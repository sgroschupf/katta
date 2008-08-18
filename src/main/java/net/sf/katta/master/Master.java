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
package net.sf.katta.master;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.DeployedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.node.NodeMetaData;
import net.sf.katta.util.ComparisonUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.MasterConfiguration;
import net.sf.katta.util.NetworkUtil;
import net.sf.katta.zk.IZkChildListener;
import net.sf.katta.zk.IZkDataListener;
import net.sf.katta.zk.ZKClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class Master {

	protected final static Logger LOG = Logger.getLogger(Master.class);

	private static final byte NOT_DEPLOYED = 0;
	private static final byte DEPLOYED = 1;
	private static final byte DEPLOYED_WITH_ERRORS = 2;

	protected ZKClient _zkClient;

	protected List<String> _nodes = new ArrayList<String>();
	protected List<String> _indexes = new ArrayList<String>();

	private IDeployPolicy _policy;
	private boolean _isMaster;

	public Master(final ZKClient zkClient) throws KattaException {
		_zkClient = zkClient;
		final MasterConfiguration masterConfiguration = new MasterConfiguration();
		final String deployPolicy = masterConfiguration.getDeployPolicy();
		try {
			final Class<IDeployPolicy> policyClazz = (Class<IDeployPolicy>) Class
					.forName(deployPolicy);
			_policy = policyClazz.newInstance();
		} catch (final Exception e) {
			throw new KattaException("Unable to instantiate deploy policy", e);
		}
	}

	public void start() throws KattaException {
		if (!_zkClient.isStarted()) {
			_zkClient.start(300000);
		}
		if (becomeMaster()) {
			// master
			// Announce me as master
			// boot up loading nodes and indexes..
			loadNodes();
			loadIndexes();
			_isMaster = true;
		} else {
			_isMaster = false;
			// secondary master
			_zkClient.subscribeDataChanges(IPaths.MASTER, new MasterListener());
			LOG.info("Secondary Master started...");
		}
	}

	public void shutdown() {
		// TODO jz: do decommission first?
		_zkClient.close();
	}

	private boolean becomeMaster() {
		synchronized (_zkClient.getSyncMutex()) {
			try {
				final String hostName = NetworkUtil.getLocalhostName();
				final MasterMetaData freshMaster = new MasterMetaData(hostName,
						System.currentTimeMillis());
				// no master so this one will be master
				if (!_zkClient.exists(IPaths.MASTER)) {
					_zkClient.createEphemeral(IPaths.MASTER, freshMaster);
					LOG.info("Master " + hostName + " started....");
					return true;
				}
				return false;
			} catch (final KattaException e) {
				throw new RuntimeException(
						"Failed to communicate with ZooKeeper", e);
			}
		}
	}

	private void loadIndexes() throws KattaException {
		LOG.debug("Loading indexes...");
		synchronized (_zkClient.getSyncMutex()) {
			final ArrayList<String> indexes = _zkClient.subscribeChildChanges(
					IPaths.INDEXES, new IndexListener());
			assert indexes != null;
			if (indexes.size() > 0) {
				addIndexes(indexes);
			}
			_indexes = indexes;
		}
	}

	private void loadNodes() throws KattaException {
		LOG.info("Loading nodes...");
		waitForNodeStartup();
		synchronized (_zkClient.getSyncMutex()) {
			final ArrayList<String> children = _zkClient.subscribeChildChanges(
					IPaths.NODES, new NodeListener());
			LOG.info("Found nodes: " + children);
			assert children != null;
			_nodes = children;
		}
	}

	private void waitForNodeStartup() throws KattaException {
		LOG.info("waiting for nodes...");
		List<String> nodes = new ArrayList<String>();
		while (nodes.size() == 0) {
			nodes = _zkClient.getChildren(IPaths.NODES);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOG.error("Wait for nodes was interrupted.", e);
			}
			boolean nodesStarted = false;
			while (!nodesStarted) {
				nodesStarted = true;
				for (String node : nodes) {
					String nodePath = IPaths.NODES + "/" + node;
					NodeMetaData nodeMetaData = new NodeMetaData();
					_zkClient.readData(nodePath, nodeMetaData);
					nodesStarted = nodesStarted && !nodeMetaData.isStarting();
				}
			}
		}
	}

	protected void addIndexes(final List<String> indexes) throws KattaException {
		LOG.info("Adding indexes: " + indexes);
		for (final String index : indexes) {
			final IndexMetaData metaData = new IndexMetaData();
			_zkClient.readData(IPaths.INDEXES + "/" + index, metaData);
			try {
				deployIndex(index, metaData);
			} catch (Exception e) {
				LOG.error("Cannot deploy index '" + index + "'.", e);
			}
		}
	}

	private void deployIndex(final String index, final IndexMetaData metaData)
			throws KattaException {
		final ArrayList<AssignedShard> shards = getShardsForIndex(index,
				metaData);
		final String indexPath = IPaths.INDEXES + "/" + index;
		if (shards.size() == 0) {
			metaData.setState(IndexMetaData.IndexState.NO_VALID_KATTA_INDEX);
			try {
				_zkClient.writeData(indexPath, metaData);
			} catch (final KattaException ke) {
				throw new RuntimeException(
						"Failed to write Index no valid katta index", ke);
			}
			throw new IllegalArgumentException(
					"No shards in folder found, this is not a valid katta virtual index.");
		}
		LOG.info("Deploying index: " + index + " [" + shards + "]");
		// add shards to index..
		for (final AssignedShard shard : shards) {
			final String path = indexPath + "/" + shard.getShardName();
			if (!_zkClient.exists(path)) {
				_zkClient.create(path, shard);
			}
		}

		// compute how to distribute shards to nodes
		final List<String> readNodes = readNodes();
		if (readNodes != null && readNodes.size() > 0) {
			final Map<String, List<AssignedShard>> distributionMap = _policy
					.distribute(_zkClient, readNodes, shards, metaData
							.getReplicationLevel());
			assignShards(distributionMap);
			try {
				metaData.setState(IndexMetaData.IndexState.ANNOUNCED);
				_zkClient.writeData(indexPath, metaData);
			} catch (final KattaException ke) {
				throw new RuntimeException("Failed to write Index announced",
						ke);
			}
			// lets have a thread watching deployment is things are done we set
			// the
			// flag in the meta data...
			new Thread() {
				@Override
				public void run() {
					LOG.info("Wait for index '" + index + "' to be deployed.");
					try {
						byte deployResult = NOT_DEPLOYED;
						while ((deployResult = isDeployedAsExpected(distributionMap)) == NOT_DEPLOYED) {
							try {
								LOG.info("Index '" + index
										+ "' not yet fully deployed, waiting");
								Thread.sleep(2000);
								try {
									if (!_zkClient.exists(indexPath)) {
										LOG
												.warn("Index '"
														+ index
														+ "' removed before the deployment completed.");
										return;
									}
								} catch (KattaException e) {
									LOG
											.warn("Error on getting katta state from zookeeper. A possbile temporary connection loss so ignoring.");
								}
							} catch (final InterruptedException e) {
								LOG.error("Deployment process was interrupted",
										e);
								return;
							}
						}
						_zkClient.readData(indexPath, metaData);
						if (deployResult == DEPLOYED_WITH_ERRORS) {
							LOG
									.error("deploy of index '" + index
											+ "' failed.");
							metaData
									.setState(IndexMetaData.IndexState.DEPLOY_ERROR);
						} else {
							LOG.info("Finnaly the index '" + index
									+ "' is deployed...");
							metaData
									.setState(IndexMetaData.IndexState.DEPLOYED);
						}
						_zkClient.writeData(indexPath, metaData);
					} catch (final KattaException e) {
						LOG.error("deploy of index '" + index + "' failed.", e);
						metaData
								.setState(IndexMetaData.IndexState.DEPLOY_ERROR);
						try {
							_zkClient.writeData(indexPath, metaData);
						} catch (final KattaException ke) {
							throw new RuntimeException(
									"Failed to write Index deployError", ke);
						}
					}
				}
			}.start();
		} else {
			LOG.warn("No nodes announced to deploy an index to.");
		}
	}

	public ArrayList<AssignedShard> getShardsForIndex(final String index,
			final IndexMetaData metaData) {
		final String pathString = metaData.getPath();
		// get shard folders from source
		URI uri;
		try {
			uri = new URI(pathString);
		} catch (final URISyntaxException e) {
			throw new IllegalArgumentException(
					"unable to parse index path uri, make sure it starts with file:// or hdfs:// ",
					e);
		}
		FileSystem fileSystem;
		try {
			fileSystem = FileSystem.get(uri, new Configuration());
		} catch (final IOException e) {
			throw new IllegalArgumentException(
					"unable to retrive file system, make sure your path starts with hadoop support prefix like file:// or hdfs://");
		}
		final Path path = new Path(pathString);
		final ArrayList<AssignedShard> shards = new ArrayList<AssignedShard>();
		try {
			final FileStatus[] listStatus = fileSystem.listStatus(path,
					new PathFilter() {
						public boolean accept(final Path aPath) {
							return !aPath.getName().startsWith(".");
						}
					});
			for (final FileStatus fileStatus : listStatus) {
				if (fileStatus.isDir()
						|| fileStatus.getPath().toString().endsWith(".zip")) {
					shards.add(new AssignedShard(index, fileStatus.getPath()
							.toString()));
				}
			}
		} catch (final IOException e) {
			throw new RuntimeException("unable to list index path: "
					+ pathString, e);
		}
		return shards;
	}

	protected byte isDeployedAsExpected(
			final Map<String, List<AssignedShard>> distributionMap)
			throws KattaException {
		byte result = NOT_DEPLOYED;
		int all = 0;
		int deployed = 0;
		int notDeployed = 0;
		int error = 0;
		final Set<String> nodes = distributionMap.keySet();
		for (final String node : nodes) {
			// which shards this node should serve..
			final List<AssignedShard> shardsToServe = distributionMap.get(node);
			all += shardsToServe.size();
			for (final AssignedShard expectedShard : shardsToServe) {
				// lookup who is actually serving this shard.
				String shardPath = IPaths.SHARD_TO_NODE + "/"
						+ expectedShard.getShardName();
				final List<String> servingNodes = _zkClient
						.getChildren(shardPath);
				// is the node we expect here already?
				boolean asExpected = false;
				for (final String servingNode : servingNodes) {
					if (node.equals(servingNode)) {
						asExpected = true;
						DeployedShard deployedShard = new DeployedShard();
						_zkClient.readData(shardPath + "/" + servingNode,
								deployedShard);
						if (deployedShard.hasError()) {
							error++;
						} else {
							deployed++;
						}
						break;
					}
				}
				if (!asExpected) {
					// node is not yet serving shard
					notDeployed++;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Shard '" + expectedShard
								+ "' not yet deployed on node '" + node + "'.");
					}
				}
			}

		}
		LOG.info("deploying: " + deployed + " of " + all
				+ " deployed, pending: " + notDeployed + ", error: " + error);
		// all as expected..

		if ((deployed + error) == all) {
			if (error > 0) {
				result = DEPLOYED_WITH_ERRORS;
			} else {
				result = DEPLOYED;
			}
		}
		return result;
	}

	private void assignShards(
			final Map<String, List<AssignedShard>> distributionMap)
			throws KattaException {
		final Set<String> nodes = distributionMap.keySet();
		for (final String node : nodes) {

			final List<AssignedShard> shardsToServer = distributionMap
					.get(node);
			for (final AssignedShard shard : shardsToServer) {
				// shard to server
				final String shardName = shard.getShardName();
				final String shardServerPath = IPaths.SHARD_TO_NODE + "/"
						+ shardName;
				if (!_zkClient.exists(shardServerPath)) {
					_zkClient.create(shardServerPath);
				}
				// node to shard
				LOG.info("Assigning:" + shardName + " to: " + node);
				final String nodePath = IPaths.NODE_TO_SHARD + "/" + node;
				final String nodeShardPath = nodePath + "/" + shardName;
				if (!_zkClient.exists(nodeShardPath)) {
					_zkClient.create(nodeShardPath, shard);
				}
			}
		}
	}

	protected void removeNodes(final List<String> removedNodes)
			throws KattaException {
		for (final String node : removedNodes) {
			// get the shards this node served...
			final String nodeToRemove = IPaths.NODE_TO_SHARD + "/" + node;
			final List<String> toAsignShards = _zkClient
					.getChildren(nodeToRemove);
			final List<AssignedShard> shards = new ArrayList<AssignedShard>();
			for (final String shardName : toAsignShards) {
				final AssignedShard metaData = new AssignedShard();
				_zkClient.readData(nodeToRemove + "/" + shardName, metaData);
				shards.add(metaData);
			}
			_zkClient.deleteRecursive(nodeToRemove);
			if (toAsignShards.size() != 0) {
				// since we lost one shard, we want to use replication level 1,
				// since
				// all other replica still exists..
				List<String> nodes = readNodes();
				if (nodes.size() > 0) {
					final Map<String, List<AssignedShard>> asignmentMap = _policy
							.distribute(_zkClient, nodes, shards, 1);
					assignShards(asignmentMap);
				} else {
					LOG.warn("No nodes left for shard redistribution.");
				}
			}
			// assign this to new nodes
		}
	}

	protected void removeIndexes(final List<String> removedIndexes) {
		LOG.debug("Remove indexes.");
		for (final String indexName : removedIndexes) {
			try {
				removeIndex(indexName);
			} catch (final KattaException e) {
				LOG.error("Failed to remove index: " + indexName, e);
			}
		}
	}

	/*
	 * iterates through all nodes and removes the assigned shards for given
	 * indexName
	 * 
	 * @throws KattaException
	 */
	private void removeIndex(final String indexName) throws KattaException {
		synchronized (_zkClient.getSyncMutex()) {
			LOG.debug("Remove index: '" + indexName + "'.");
			final List<String> nodes = _zkClient
					.getChildren(IPaths.NODE_TO_SHARD);
			for (final String node : nodes) {
				final String nodePath = IPaths.NODE_TO_SHARD + "/" + node;
				final List<String> assignedShards = _zkClient
						.getChildren(nodePath);
				for (final String shard : assignedShards) {
					final AssignedShard shardWritable = new AssignedShard();
					final String shardPath = nodePath + "/" + shard;
					_zkClient.readData(shardPath, shardWritable);
					if (shardWritable.getIndexName()
							.equalsIgnoreCase(indexName)) {
						_zkClient.delete(shardPath);
					}
				}
			}
		}
	}

	protected class NodeListener implements IZkChildListener {

		public void handleChildChange(String parentPath) throws KattaException {
			LOG.info("Node event.");
			synchronized (_zkClient.getSyncMutex()) {
				List<String> currentNodes;
				try {
					currentNodes = _zkClient.getChildren(parentPath);
					final List<String> disconnectedNodes = ComparisonUtil
							.getRemoved(_nodes, currentNodes);
					if (!disconnectedNodes.isEmpty()) {
						LOG.info(disconnectedNodes.size()
								+ " node/s disconnected: " + disconnectedNodes);
						// removeNodes(disconnectedNodes);
						// jz: we don't remove the node-2-shard mapping since
						// the node might reconnect in a short time and then it
						// won't get any shards assigned (only on new
						// deployments). Once we have a shard balancing tast we
						// should enable this in one or another form (idea: see
						// DelayQueue). Since we have replication a down node
						// should not affect the shard availibility too much.
					}
					List<String> newNodes = ComparisonUtil.getNew(_nodes,
							currentNodes);
					if (!newNodes.isEmpty()) {
						LOG.info(newNodes.size() + " new node/s connected: "
								+ newNodes);
					}
					_nodes = currentNodes;
				} catch (final KattaException e) {
					throw new RuntimeException("Faled to read zookeeper data.",
							e);
				} finally {
					_zkClient.getSyncMutex().notifyAll();
				}
			}
		}
	}

	protected class IndexListener implements IZkChildListener {

		public void handleChildChange(String parentPath) throws KattaException {
			LOG.info("Indexes event.");
			synchronized (_zkClient.getSyncMutex()) {
				List<String> freshIndexes;
				try {
					freshIndexes = _zkClient.getChildren(parentPath);
					final List<String> removedIndices = ComparisonUtil
							.getRemoved(_indexes, freshIndexes);
					removeIndexes(removedIndices);
					final List<String> newIndexes = ComparisonUtil.getNew(
							_indexes, freshIndexes);
					addIndexes(newIndexes);
					_indexes = freshIndexes;
				} catch (final KattaException e) {
					throw new RuntimeException("Failed to read zookeeper data",
							e);
				} finally {
					_zkClient.getSyncMutex().notifyAll();
				}
			}
		}
	}

	protected class MasterListener implements IZkDataListener {

		public void handleDataChange(String parentPath) throws KattaException {
			synchronized (_zkClient.getSyncMutex()) {
				// start from scratch again...
				LOG.info("An master failure was detected...");
				try {
					start();
				} catch (final KattaException e) {
					LOG
							.error(
									"Faild to process Master change notificaiton.",
									e);
				}
			}
		}
	}

	protected List<String> readNodes() throws KattaException {
		return _zkClient.getChildren(IPaths.NODES);
	}

	protected List<String> readIndexes() throws KattaException {
		return _zkClient.getChildren(IPaths.INDEXES);
	}

	@Override
	protected void finalize() throws Throwable {
		_zkClient.delete(IPaths.MASTER);
		super.finalize();
	}

	public boolean isMaster() {
		return _isMaster;
	}

	public List<String> getNodes() {
		return Collections.unmodifiableList(_nodes);
	}

}
