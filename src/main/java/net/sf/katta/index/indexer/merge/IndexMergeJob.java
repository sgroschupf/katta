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
package net.sf.katta.index.indexer.merge;

import java.util.Arrays;

import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

public class IndexMergeJob implements Configurable {

  private final static Logger LOG = Logger.getLogger(IndexMergeJob.class);

  private Configuration _configuration;

  /**
   * Merges all index shards inside the given kattaIndices path to one index
   * with optimal shard count. Note that the kattaIndices path can span multiple
   * indexes.
   */
  public void merge(Path kattaIndices, Path ouputPath) throws Exception {
    merge(new Path[] { kattaIndices }, ouputPath);
  }

  /**
   * Merges the shards from all given kattaIndices to one index with optimal
   * shard count.
   */
  public void merge(Path[] kattaIndices, Path ouputPath) throws Exception {
    LOG.info("merge indices " + Arrays.asList(kattaIndices) + " to " + ouputPath);

    Path dedupPath = new Path("/tmp/katta.index.dedup", "" + System.currentTimeMillis());

    _configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    IndexToSequenceFileJob indexToSequenceFileJob = new IndexToSequenceFileJob();
    indexToSequenceFileJob.setConf(_configuration);
    indexToSequenceFileJob.indexToSequenceFile(kattaIndices, dedupPath);

    SequenceFileToIndexJob sequenceFileToIndexJob = new SequenceFileToIndexJob();
    sequenceFileToIndexJob.setConf(_configuration);
    sequenceFileToIndexJob.sequenceFileToIndex(dedupPath, ouputPath);

    LOG.info("delete sequence file and extracted indices: " + dedupPath);
    FileSystem fileSystem = FileSystem.get(_configuration);
    fileSystem.delete(dedupPath);
    LOG.info("merging done. find the result here: " + ouputPath);
  }

  public void setConf(Configuration configuration) {
    _configuration = configuration;
  }

  public Configuration getConf() {
    return _configuration;
  }

  public static void main(String[] args) throws Exception {
    Path kattaIndices = new Path(args[0]);
    Path out = new Path(args[1]);

    JobConf jobConf = new JobConf();
    IndexMergeJob job = new IndexMergeJob();
    jobConf.setJarByClass(IndexMergeJob.class);
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.enrichJobConf(jobConf, DfsIndexInputFormat.DOCUMENT_INFORMATION);
    indexConfiguration.enrichJobConf(jobConf, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);

    job.setConf(jobConf);
    job.merge(kattaIndices, out);
  }
}
