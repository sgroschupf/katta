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

package net.sf.katta.index.indexer.merge;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.IndexConfiguration;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ZkConfiguration;
import net.sf.katta.zk.ZKClient;
import net.sf.katta.zk.ZkPathes;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

public class IndexMergeApplication {

  private final static Logger LOG = Logger.getLogger(IndexMergeApplication.class);

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd.hhmmss");

  private final JobConf _jobConf;

  private final ZKClient _zkClient;

  // public IndexMergeApplication(String hadoopJobtracker, String jobJar) {
  public IndexMergeApplication(ZKClient zkcClient) {
    _zkClient = zkcClient;
    _jobConf = new JobConf();
    _jobConf.setJarByClass(IndexMergeJob.class);
    // jobConf.set("mapred.job.tracker", hadoopJobtracker);
    // jobConf.set("mapred.job.tracker", hadoopJobtracker);
    IndexMergeJob.enrichJobConf(_jobConf, new IndexConfiguration());
  }

  public void merge(String[] indexesToMerge) throws Exception {
    mergeIndices(new DeployClient(_zkClient), Arrays.asList(indexesToMerge));
  }

  public void mergeDeployedIndices() throws Exception {
    IDeployClient deployClient = new DeployClient(new ZkConfiguration());
    List<String> deployedIndexNames = deployClient.getIndexNames(IndexState.DEPLOYED);
    mergeIndices(deployClient, deployedIndexNames);
  }

  private void mergeIndices(IDeployClient deployClient, List<String> indexNames) throws Exception {
    List<IndexMetaData> deployedIndexes = new ArrayList<IndexMetaData>();
    for (String indexName : indexNames) {
      IndexMetaData indexMetaData = new IndexMetaData();
      _zkClient.readData(ZkPathes.getIndexPath(indexName), indexMetaData);
      deployedIndexes.add(indexMetaData);
    }

    Set<Path> indexPathes = new HashSet<Path>();
    for (IndexMetaData indexMetaData : deployedIndexes) {
      indexPathes.add(new Path(indexMetaData.getPath()));
    }
    LOG.info("found following indexes for potential merge: " + indexPathes);

    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.enrichJobConf(_jobConf, DfsIndexInputFormat.DOCUMENT_INFORMATION);
    indexConfiguration.enrichJobConf(_jobConf, IndexConfiguration.MAPRED_OUTPUT_PATH);

    IndexMergeJob indexMergeJob = new IndexMergeJob();
    indexMergeJob.setConf(_jobConf);

    Path uploadPath = indexConfiguration.getPath(IndexConfiguration.INDEX_UPLOAD_PATH);
    Path mergedIndex = new Path(uploadPath, "mergedIndex-" + DATE_FORMAT.format(new Date()));

    int optimalShardCount = indexConfiguration.getInt(IndexConfiguration.INDEX_SHARD_COUNT);
    int currentShardCount = countShards(indexNames);

    LOG.info("found " + currentShardCount + " shards");
    if (currentShardCount == 0) {
      LOG.warn("no shard under '" + uploadPath + "' found");
      return;
    }
    if (currentShardCount <= optimalShardCount) {
      LOG.warn("shard count is " + currentShardCount + ", optimal shard count is " + optimalShardCount
          + ". No need for merging shards.");
      return;
    }

    FileSystem fileSystem = FileSystem.get(_jobConf);
    LOG.debug("using file system: " + fileSystem.getUri());
    try {
      indexMergeJob.merge(indexPathes.toArray(new Path[indexPathes.size()]), mergedIndex);

      if (!fileSystem.exists(mergedIndex)) {
        throw new IllegalStateException("merged index '" + mergedIndex + "' does not exists");
      }

      // now deploy the new index
      mergedIndex = mergedIndex.makeQualified(fileSystem);
      LOG.info("deploying new merged index: " + mergedIndex);
      IIndexDeployFuture deployFuture = deployClient.addIndex(mergedIndex.getName(), mergedIndex.toString()
          + "/indexes", deployedIndexes.get(0).getAnalyzerClassName(), deployedIndexes.get(0).getReplicationLevel());
      // TODO jz: just taking the analyzer and replication level from the
      // first is unclean
      // TODO jz: appending / indexes is suboptimal
      IndexState indexState = deployFuture.joinDeployment();
      if (indexState == IndexState.ERROR) {
        throw new IllegalStateException("could not deploy merged index '" + mergedIndex.getName() + "'");
      }

      // now undeploy the old indices
      LOG.info("undeploying old merged indices: " + indexNames);
      for (String indexName : indexNames) {
        deployClient.removeIndex(indexName);
      }

      // now move the old indexes to archive
      Path archiveRootPath = new Path(indexConfiguration.getPath(IndexConfiguration.INDEX_ARCHIVE_PATH), mergedIndex
          .getName()
          + "-originals");
      fileSystem.mkdirs(archiveRootPath);
      LOG.info("moving old merged indices to archive: " + archiveRootPath);
      for (Path indexPath : indexPathes) {
        Path parentPath = indexPath.getParent();// parent of /indexes
        Path indexArchivePath = new Path(archiveRootPath, parentPath.getName());
        LOG.debug("moving " + parentPath + " to " + indexArchivePath);
        fileSystem.rename(parentPath, indexArchivePath);
      }
    } catch (Exception e) {
      fileSystem.delete(mergedIndex, true);
      throw e;
    }
  }

  private int countShards(List<String> indexNames) throws KattaException {
    int shardCount = 0;
    for (String index : indexNames) {
      shardCount += _zkClient.countChildren(ZkPathes.getIndexPath(index));
    }
    return shardCount;
  }

  public static void main(String[] args) throws Exception {
    ZKClient zkcClient = new ZKClient(new ZkConfiguration());
    zkcClient.start(3000);
    new IndexMergeApplication(zkcClient).mergeDeployedIndices();
  }

}
