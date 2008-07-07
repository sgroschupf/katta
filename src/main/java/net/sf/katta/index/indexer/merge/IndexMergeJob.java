package net.sf.katta.index.indexer.merge;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class IndexMergeJob implements Configurable {

  private Configuration _configuration;

  public void merge(Path kattaIndices) throws Exception {
    Path dedupPath = new Path("/tmp/katta.index.dedup", "" + System.currentTimeMillis());

    IndexToSequenceFileJob indexToSequenceFileJob = new IndexToSequenceFileJob();
    indexToSequenceFileJob.setConf(_configuration);
    indexToSequenceFileJob.indexToSequenceFile(kattaIndices, dedupPath);

    SequenceFileToIndexJob sequenceFileToIndexJob = new SequenceFileToIndexJob();
    sequenceFileToIndexJob.setConf(_configuration);
    sequenceFileToIndexJob.sequenceFileToIndex(dedupPath);
  }

  public void setConf(Configuration configuration) {
    _configuration = configuration;
  }

  public Configuration getConf() {
    return _configuration;
  }

  public static void main(String[] args) throws Exception {
    //TODO delete all merged katta indices
    Path kattaIndices = new Path(args[0]);
    IndexMergeJob job = new IndexMergeJob();
    JobConf jobConf = new JobConf();
    jobConf.setJarByClass(IndexMergeJob.class);
    job.setConf(jobConf);
    job.merge(kattaIndices);
  }
}
