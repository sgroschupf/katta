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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

public class IndexMergeJob implements Configurable {

  private final static Logger LOG = Logger.getLogger(IndexMergeJob.class);

  private Configuration _configuration;

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd.hhmmss");

  public void merge(Path kattaIndices, Path ouputPath, Path archivePath) throws Exception {
    LOG.info("collect all shards in this folder: " + kattaIndices);

    Path dedupPath = new Path("/tmp/katta.index.dedup", "" + System.currentTimeMillis());

    _configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    IndexToSequenceFileJob indexToSequenceFileJob = new IndexToSequenceFileJob();
    indexToSequenceFileJob.setConf(_configuration);
    indexToSequenceFileJob.indexToSequenceFile(kattaIndices, dedupPath);

    SequenceFileToIndexJob sequenceFileToIndexJob = new SequenceFileToIndexJob();
    sequenceFileToIndexJob.setConf(_configuration);
    final String mergedIndex = sequenceFileToIndexJob.sequenceFileToIndex(dedupPath, ouputPath);

    LOG.info("delete sequence file and extracted indices: " + dedupPath);
    FileSystem fileSystem = FileSystem.get(_configuration);
    fileSystem.delete(dedupPath);

    LOG.info("move katta content of folder'" + kattaIndices + "' into a archive folder '" + archivePath + "'");
    PathFilter filter = new PathFilter() {
      public boolean accept(Path path) {
        return (!path.getName().equals(mergedIndex));
      }
    };
    FileStatus[] fileStatuses = fileSystem.listStatus(kattaIndices, filter);

    Path currentArchivePath = new Path(archivePath, kattaIndices.getName() + "-" + DATE_FORMAT.format(new Date()));
    fileSystem.mkdirs(currentArchivePath);
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      Path renamedPath = new Path(currentArchivePath, path.getName());
      LOG.info("rename '" + path + "' to'" + renamedPath + "'");
      fileSystem.rename(path, renamedPath);
    }

    LOG.info("merging done.");
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
    Path archive = new Path(args[2]);
    IndexMergeJob job = new IndexMergeJob();
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(IndexMergeJob.class);
    job.setConf(jobConf);
    job.merge(kattaIndices, out, archive);
  }
}
