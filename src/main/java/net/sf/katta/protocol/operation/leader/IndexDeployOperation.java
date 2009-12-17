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
package net.sf.katta.protocol.operation.leader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.master.LeaderContext;
import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.protocol.metadata.IndexMetaData.Shard;
import net.sf.katta.protocol.operation.OperationId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class IndexDeployOperation extends AbstractDistributeShardsOperation {

  private static final long serialVersionUID = 1L;
  private final static Logger LOG = Logger.getLogger(IndexDeployOperation.class);

  private final String _indexName;
  private final String _indexPath;
  private final int _replicationLevel;

  public IndexDeployOperation(String indexName, String indexPath, int replicationLevel) {
    _indexName = indexName;
    _indexPath = indexPath;
    _replicationLevel = replicationLevel;
  }

  @Override
  public void execute(LeaderContext context) throws Exception {
    InteractionProtocol protocol = context.getProtocol();
    LOG.info("deploying index '" + _indexName + "'");
    IndexMetaData indexMetaData = new IndexMetaData(_indexName, _indexPath, _replicationLevel, IndexState.ANNOUNCED);// avoid

    List<Shard> readShards = readShardsFromFs(_indexName, indexMetaData);
    for (Shard shard : readShards) {
      indexMetaData.getShards().add(shard);
    }
    protocol.writeIndexMD(indexMetaData);
    LOG.info("Found shards '" + readShards + "' for index '" + _indexName + "'");
    try {
      indexMetaData.setState(IndexState.DEPLOYING);
      protocol.updateIndexMD(indexMetaData);
      List<OperationId> nodeOperationIds = distributeIndexShards(protocol, context.getDeployPolicy(), indexMetaData,
              protocol.getNodes(), true);
      context.getOperationRegistry().watchIndexDeployment(_indexName, nodeOperationIds);
    } catch (Exception e) {
      LOG.error("failed to deploy index " + _indexName, e);
      indexMetaData.setState(IndexState.ERROR, StringUtils.stringifyException(e));
      protocol.updateIndexMD(indexMetaData);
    }
  }

  @Override
  public void nodeOperationComplete(LeaderContext context) throws Exception {
    LOG.info("deployment of index " + _indexName + " complete - triggering complete deployment operation");
    // TODO index metadata to live / error

  }

  protected List<Shard> readShardsFromFs(final String index, final IndexMetaData indexMetaData)
          throws IndexInvalidException {
    final String indexPathString = indexMetaData.getPath();
    // get shard folders from source
    URI uri;
    try {
      uri = new URI(indexPathString);
    } catch (final URISyntaxException e) {
      throw new IndexInvalidException("unable to parse index path uri '" + indexPathString
              + "', make sure it starts with file:// or hdfs:// ", e);
    }
    FileSystem fileSystem;
    try {
      fileSystem = FileSystem.get(uri, new Configuration());
    } catch (final IOException e) {
      throw new IndexInvalidException("unable to retrive file system for index path '" + indexPathString
              + "', make sure your path starts with hadoop support prefix like file:// or hdfs://", e);
    }

    List<Shard> shards = new ArrayList<Shard>();
    try {
      final Path indexPath = new Path(indexPathString);
      if (!fileSystem.exists(indexPath)) {
        throw new IndexInvalidException("index path '" + uri + "' does not exists");
      }
      final FileStatus[] listStatus = fileSystem.listStatus(indexPath, new PathFilter() {
        public boolean accept(final Path aPath) {
          return !aPath.getName().startsWith(".");
        }
      });
      for (final FileStatus fileStatus : listStatus) {
        String shardPath = fileStatus.getPath().toString();
        if (fileStatus.isDir() || shardPath.endsWith(".zip")) {
          shards.add(new Shard(createShardName(index, shardPath), shardPath));
        }
      }
    } catch (final IOException e) {
      throw new IndexInvalidException("could not access index path: " + indexPathString, e);
    }

    if (shards.size() == 0) {
      throw new IndexInvalidException("index does not contain any shard");
    }
    return shards;
  }

}
