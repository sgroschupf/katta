package net.sf.katta.index.indexer.merge;

import java.io.InputStream;

import net.sf.katta.index.indexer.IndexJobConf;
import net.sf.katta.index.indexer.ShardSelectionMapper;
import net.sf.katta.util.Logger;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public class SequenceFileToIndexJob implements Configurable {

  private Configuration _configuration;

  public void sequenceFileToIndex(Path sequenceFilePath, Path outputFolder) throws Exception {

    InputStream resourceAsStream = SequenceFileToIndexJob.class.getResourceAsStream("/katta.index.properties");
    JobConf jobConf = new IndexJobConf().create(_configuration, resourceAsStream);

    jobConf.setJobName("SequenceFileToIndex");

    // input and output format
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    // no output format will be reduced because the index will be  creat local and will be copied into the hds

    // input and output path
    //overwrite the settet input path which is set via IndexJobConf.create
    jobConf.set("mapred.input.dir", "");
    Logger.info("read document informations from sequence file: " + sequenceFilePath);
    jobConf.addInputPath(sequenceFilePath);

    //configure the mapper
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    jobConf.setMapperClass(ShardSelectionMapper.class);
    //the input key and input value class which is saved in the sequence file will be mapped out as value: BytesWritable
    jobConf.set("index.input.key.class", Text.class.getName());
    jobConf.set("index.input.value.class", DocumentInformation.class.getName());
    String indexFolder = "" + System.currentTimeMillis() + "-merge";

    Path newOutputPath = new Path(jobConf.getOutputPath(), indexFolder);
    Logger.info("set mapred folder to: " + newOutputPath);
    jobConf.setOutputPath(newOutputPath);

    String uploadPath = outputFolder.toString() +"/" + indexFolder;
    Logger.info("set index upload folder: '" + uploadPath + "'");
    jobConf.set(IndexJobConf.INDEX_UPLOAD_PATH, uploadPath);

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
    mergeJob.setConf(jobConf);
    mergeJob.sequenceFileToIndex(new Path(args[0]), new Path("/tmp/" + System.currentTimeMillis()));

  }
}