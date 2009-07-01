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
package net.sf.katta.indexing;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;

/**
 * Illustrates how to implement a indexer as hadoop map reduce job.
 */
public class IndexerJob {

  public static void main(String[] args) throws IOException {

    if (args.length != 3) {
      String usage = "IndexerJob <in text file/dir> <out katta index dir> <numOfShards>";
      System.out.println(usage);
      System.exit(1);
    }

    IndexerJob indexerJob = new IndexerJob();
    String input = args[0];
    String output = args[1];
    int numOfShards = Integer.parseInt(args[2]);
    indexerJob.startIndexer(input, output, numOfShards);

  }

  public void startIndexer(String path, String finalDestination, int numOfShards) throws IOException {
    // create job conf with class pointing into job jar.
    JobConf jobConf = new JobConf(IndexerJob.class);
    jobConf.setJobName("indexer");
    jobConf.setMapRunnerClass(Indexer.class);
    // alternative use a text file and a TextInputFormat
    jobConf.setInputFormat(SequenceFileInputFormat.class);

    Path input = new Path(path);
    SequenceFileInputFormat.setInputPaths(jobConf, input);
    // we just set the output path to make hadoop happy.
    TextOutputFormat.setOutputPath(jobConf, new Path(finalDestination));
    // setting the folder where lucene indexes will be copied when finished.
    jobConf.set("finalDestination", finalDestination);
    // important to switch spec exec off.
    // We dont want to have something duplicated.
    jobConf.setSpeculativeExecution(false);

    // The num of map tasks is equal to the num of input splits.
    // The num of input splits by default is equal to the num of hdf blocks
    // for the input file(s). To get the right num of shards we need to
    // calculate the best input split size.

    FileSystem fs = FileSystem.get(input.toUri(), jobConf);
    FileStatus[] status = fs.globStatus(input);
    long size = 0;
    for (FileStatus fileStatus : status) {
      size += fileStatus.getLen();
    }
    long optimalSplisize = size / numOfShards;
    jobConf.set("mapred.min.split.size", "" + optimalSplisize);

    // give more mem to lucene tasks.
    jobConf.set("mapred.child.java.opts", "-Xmx2G");
    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(0);
    JobClient.runJob(jobConf);
  }

  public static class Indexer implements MapRunnable<LongWritable, Text, Text, Text> {

    private JobConf _conf;

    public void configure(JobConf conf) {
      _conf = conf;

    }

    @SuppressWarnings("deprecation")
    public void run(RecordReader<LongWritable, Text> reader, OutputCollector<Text, Text> output, final Reporter report)
        throws IOException {
      LongWritable key = reader.createKey();
      Text value = reader.createValue();

      String tmp = _conf.get("hadoop.tmp.dir");
      long millis = System.currentTimeMillis();
      String shardName = "" + millis + "-" + new Random().nextInt();
      File file = new File(tmp, shardName);
      report.progress();
      Analyzer analyzer = IndexConfiguration.getAnalyzer(_conf);
      IndexWriter indexWriter = new IndexWriter(file, analyzer);
      indexWriter.setMergeFactor(100000);
      report.setStatus("Adding documents...");
      while (reader.next(key, value)) {
        report.progress();
        Document doc = new Document();
        String text = "" + value.toString();
        Field contentField = new Field("content", text, Store.YES, Index.TOKENIZED);
        doc.add(contentField);
        indexWriter.addDocument(doc);
      }

      report.setStatus("Done adding documents.");
      Thread t = new Thread() {
        public boolean stop = false;

        @Override
        public void run() {
          while (!stop) {
            // Makes sure hadoop is not killing the task in case the
            // optimization
            // takes longer than the task timeout.
            report.progress();
            try {
              sleep(10000);
            } catch (InterruptedException e) {
              // don't need to do anything.
              stop = true;
            }
          }
        }
      };
      t.start();
      report.setStatus("Optimizing index...");
      indexWriter.optimize();
      report.setStatus("Done optimizing!");
      report.setStatus("Closing index...");
      indexWriter.close();
      report.setStatus("Closing done!");
      FileSystem fileSystem = FileSystem.get(_conf);

      report.setStatus("Starting copy to final destination...");
      Path destination = new Path(_conf.get("finalDestination"));
      fileSystem.copyFromLocalFile(new Path(file.getAbsolutePath()), destination);
      report.setStatus("Copy to final destination done!");
      report.setStatus("Deleting tmp files...");
      FileUtil.fullyDelete(file);
      report.setStatus("Delteing tmp files done!");
      t.interrupt();
    }
  }
}
