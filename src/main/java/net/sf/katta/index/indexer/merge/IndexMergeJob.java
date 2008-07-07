package net.sf.katta.index.indexer.merge;

import org.apache.hadoop.fs.Path;

public class IndexMergeJob {


  public void merge(Path kattaIndices) throws Exception {
    Path dedupPath = new Path("/tmp/katta.index.dedup");

    IndexToSequenceFileJob indexToSequenceFileJob = new IndexToSequenceFileJob();
    indexToSequenceFileJob.indexToSequenceFile(kattaIndices, dedupPath);

    SequenceFileToIndexJob sequenceFileToIndexJob = new SequenceFileToIndexJob();
    sequenceFileToIndexJob.sequenceFileToIndex(dedupPath);
  }

  public static void main(String[] args) throws Exception {
    //TODO delete all merged katta indices
    Path kattaIndices = new Path(args[0]);
    IndexMergeJob job = new IndexMergeJob();
    job.merge(kattaIndices);
  }

}
