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

import java.io.IOException;

import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.log4j.Logger;

public class IndexToSequenceFileJob implements Configurable {

  private final static Logger LOG = Logger.getLogger(IndexToSequenceFileJob.class);

  private Configuration _configuration;

  public void indexToSequenceFile(Path[] indexPathes, Path outputPath) throws IOException {
    JobConf jobConf = new JobConf(_configuration);
    jobConf.setJobName("IndexToSequenceFile");

    // input and output format
    jobConf.setInputFormat(DfsIndexInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    // input and output path
    for (Path indexPath : indexPathes) {
      LOG.info("read all shards from folder: " + indexPath);
      jobConf.addInputPath(indexPath);
    }
    LOG.info("write sequence file to: " + outputPath);
    jobConf.setOutputPath(outputPath);

    // set input and output key/value class
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(DocumentInformation.class);

    // mapper and reducer
    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IndexDuplicateReducer.class);

    // run the job
    JobClient.runJob(jobConf);
  }

  public void setConf(Configuration configuration) {
    _configuration = configuration;
  }

  public Configuration getConf() {
    return _configuration;
  }

  public static void main(String[] args) throws IOException {
    IndexToSequenceFileJob index = new IndexToSequenceFileJob();
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(IndexToSequenceFileJob.class);
    new IndexConfiguration().enrichJobConf(jobConf, DfsIndexInputFormat.DOCUMENT_INFORMATION);

    index.setConf(jobConf);
    index.indexToSequenceFile(new Path[] { new Path(args[0]) }, new Path(args[1]));
  }

}
