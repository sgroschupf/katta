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

import net.sf.katta.index.indexer.ShardSelectionMapper;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.log4j.Logger;

public class SequenceFileToIndexJob implements Configurable {

  private final static Logger LOG = Logger.getLogger(SequenceFileToIndexJob.class);

  private Configuration _configuration;

  public void sequenceFileToIndex(Path sequenceFilePath, Path outputFolder) throws Exception {
    JobConf jobConf = new IndexConfiguration().createJobConf(_configuration);

    jobConf.setJobName("SequenceFileToIndex");

    // input and output format
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    // no output format will be reduced because the index will be creat local
    // and will be copied into the hds

    // input and output path
    // overwrite the settet input path which is set via IndexJobConf.create
    jobConf.set("mapred.input.dir", "");
    LOG.info("read document informations from sequence file: " + sequenceFilePath);
    jobConf.addInputPath(sequenceFilePath);

    // configure the mapper
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    jobConf.setMapperClass(ShardSelectionMapper.class);
    // the input key and input value class which is saved in the sequence file
    // will be mapped out as value: BytesWritable
    jobConf.set("index.input.key.class", Text.class.getName());
    jobConf.set("index.input.value.class", DocumentInformation.class.getName());

    Path newOutputPath = new Path(jobConf.get(IndexConfiguration.MAPRED_OUTPUT_PATH), outputFolder.getName());
    LOG.info("set mapred folder to: " + newOutputPath);
    jobConf.setOutputPath(newOutputPath);

    LOG.info("set index upload folder: '" + outputFolder + "'");
    jobConf.set(IndexConfiguration.INDEX_UPLOAD_PATH, outputFolder.toString());

    jobConf.set("document.factory.class", DfsIndexDocumentFactory.class.getName());

    // run the job
    JobClient.runJob(jobConf);
  }

  public void setConf(Configuration configuration) {
    _configuration = configuration;
  }

  public Configuration getConf() {
    return _configuration;
  }

  public static void main(String[] args) throws Exception {
    SequenceFileToIndexJob mergeJob = new SequenceFileToIndexJob();
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(SequenceFileToIndexJob.class);
    new IndexConfiguration().enrichJobConf(jobConf, IndexConfiguration.INDEX_SHARD_KEY_GENERATOR_CLASS);

    mergeJob.setConf(jobConf);
    mergeJob.sequenceFileToIndex(new Path(args[0]), new Path("/tmp/" + System.currentTimeMillis()));

  }
}
