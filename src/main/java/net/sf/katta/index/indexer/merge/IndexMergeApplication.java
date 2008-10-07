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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.sf.katta.client.DeployClient;
import net.sf.katta.client.IDeployClient;
import net.sf.katta.client.IIndexDeployFuture;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.index.IndexMetaData.IndexState;
import net.sf.katta.util.IndexConfiguration;
import net.sf.katta.util.ZkConfiguration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

public class IndexMergeApplication {

  private final static Logger LOG = Logger.getLogger(IndexMergeApplication.class);

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd.hhmmss");

  public void mergeDeployedIndices(JobConf jobConf) throws Exception {
    IDeployClient deployClient = new DeployClient(new ZkConfiguration());
    List<IndexMetaData> deployedIndexes = deployClient.getIndexes(IndexState.DEPLOYED);
    List<String> deployedIndexNames = deployClient.getIndexNames(IndexState.DEPLOYED);

    List<Path> indexPathes = new ArrayList<Path>();
    for (IndexMetaData indexMetaData : deployedIndexes) {
      indexPathes.add(new Path(indexMetaData.getPath()));
    }
    LOG.info("found following indexes for potential merge: " + indexPathes);

    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.enrichJobConf(jobConf, DfsIndexInputFormat.DOCUMENT_INFORMATION);
    indexConfiguration.enrichJobConf(jobConf, IndexConfiguration.MAPRED_OUTPUT_PATH);

    IndexMergeJob indexMergeJob = new IndexMergeJob();
    indexMergeJob.setConf(jobConf);

    Path uploadPath = indexConfiguration.getPath(IndexConfiguration.INDEX_UPLOAD_PATH);
    Path mergedIndex = new Path(uploadPath, "mergedIndex-" + DATE_FORMAT.format(new Date()));

    int optimalShardCount = indexConfiguration.getInt(IndexConfiguration.INDEX_SHARD_COUNT);
    int currentShardCount = countShards(jobConf, uploadPath);

    if (currentShardCount == 0) {
      LOG.warn("no shard under '" + uploadPath + "' found");
      return;
    }
    if (currentShardCount <= optimalShardCount) {
      LOG.warn("shard count is " + currentShardCount + ", optimal shard count is " + optimalShardCount
          + ". No need for merging shards.");
      return;
    }

    FileSystem fileSystem = FileSystem.get(jobConf);
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
        throw new IllegalStateException("could not deploy index '" + mergedIndex.getName() + "'");
      }

      // now undeploy the old indices
      LOG.info("undeploying old merged indices: " + deployedIndexNames);
      for (String indexName : deployedIndexNames) {
        deployClient.removeIndex(indexName);
      }

      // now move the old indexes to archive
      Path archiveRootPath = new Path(indexConfiguration.getPath(IndexConfiguration.INDEX_ARCHIVE_PATH), mergedIndex
          .getName()
          + "-originals");
      fileSystem.mkdirs(archiveRootPath);
      LOG.info("moving old merged indices to archive: " + archiveRootPath);
      for (IndexMetaData indexMetaData : deployedIndexes) {
        Path indexPath = new Path(indexMetaData.getPath().substring(0,
            indexMetaData.getPath().length() - "/indexes".length()));
        Path indexArchivePath = new Path(archiveRootPath, indexPath.getName());
        LOG.debug("moving " + indexPath + " to " + indexArchivePath);
        fileSystem.rename(indexPath, indexArchivePath);
      }
    } catch (Exception e) {
      fileSystem.delete(mergedIndex, true);
      throw e;
    }
  }

  private int countShards(JobConf jobConf, Path uploadPath) throws IOException {
    FileSystem fileSystem = FileSystem.get(jobConf);
    if (!fileSystem.exists(uploadPath)) {
      return 0;
    }
    FileStatus[] globStatus = fileSystem.globStatus(new Path(uploadPath + "/*/*/*/*/*.zip"));
    return globStatus.length;
  }

  public static void main(String[] args) throws Exception {
    // TODO fileSystemPath is not needed because we get indices from katta
    // client
    // TODO extend jobConf to get a DistributedFileSystem instead a
    // LocalFileSystem
    Options options = new Options();
    options.addOption("jobTrackerHost", true, "Hostname where the Jobtracker is running");
    options.addOption("fileSystemPath", true, "Path to the indices");
    options.addOption("jobJar", true, "Path to the job jar file");
    
    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = parser.parse(options, args);

    if (!commandLine.hasOption("jobTrackerHost") || !commandLine.hasOption("fileSystemPath")
        || !commandLine.hasOption("jobJar")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("merge", options);
      return;
    }
    
    JobConf jobConf = new JobConf();
    jobConf.setJar(commandLine.getOptionValue("jobJar"));
    jobConf.setJobName("merge indices");
    
    IndexMergeApplication application = new IndexMergeApplication();
    application.mergeDeployedIndices(jobConf);
  }

}
