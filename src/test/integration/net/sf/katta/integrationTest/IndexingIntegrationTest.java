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
package net.sf.katta.integrationTest;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import net.sf.katta.testutil.HadoopTestUtil;
import net.sf.katta.testutil.TestDocumentFactory;
import net.sf.katta.testutil.HadoopTestUtil.ITestDocumentRecordGenerator;
import net.sf.katta.testutil.HadoopTestUtil.TestDocumentRecord;
import net.sf.katta.util.FileUtil;
import net.sf.katta.util.IHadoopConstants;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.index.IndexReader;

public class IndexingIntegrationTest extends TestCase {

  private boolean GRID_MODE = true;// hadoop local or cluster mode

  private int _tasktrackerCount = 4;
  int _documentCount = 23000;
  private HadoopMiniCluster _hadoopCluster;
  private JobConf _jobConf;

  @Override
  protected void setUp() throws Exception {
    if (GRID_MODE) {
      _hadoopCluster = new HadoopMiniCluster(9000, 9001, 2, _tasktrackerCount);
      _hadoopCluster.start();
      _jobConf = _hadoopCluster.createJobConf();
    } else {
      _jobConf = new JobConf();
      _jobConf.set(IHadoopConstants.IO_SORT_MB, "20");
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (GRID_MODE) {
      _hadoopCluster.stop();
    }
  }

  public void testIndexing() throws Exception {
    Path inputPath = new Path("/tmp/indexInput");
    Path outputPath = new Path("/tmp/indexOutput");
    Path publishPath = new Path("/tmp/indexPublished");

    _jobConf = new IndexConfiguration().createJobConf(_jobConf);
    _jobConf.set(IndexConfiguration.DOCUMENT_FACTORY_CLASS, TestDocumentFactory.class.getName());
    _jobConf.set(IndexConfiguration.INPUT_KEY_CLASS, Text.class.getName());
    _jobConf.set(IndexConfiguration.INPUT_VALUE_CLASS, MapWritable.class.getName());
    _jobConf.set(IndexConfiguration.INDEX_UPLOAD_PATH, publishPath.toString());
    _jobConf.setInt(IndexConfiguration.INDEX_SHARD_COUNT, _tasktrackerCount);

    FileSystem fileSystem = FileSystem.get(_jobConf);
    fileSystem.delete(outputPath);
    writeDocumentRecordSequenceFile(_jobConf, inputPath, _documentCount);
    _jobConf.setInputPath(inputPath);
    _jobConf.setOutputPath(outputPath);

    JobClient.runJob(_jobConf);

    FileStatus[] zippedShards = fileSystem.listStatus(new Path(publishPath, "indexes"));
    int indexDocCount = 0;
    assertEquals(_tasktrackerCount, zippedShards.length);
    for (FileStatus fileStatus : zippedShards) {
      File localShardZip = new File("/tmp/downloadedShard.zip");
      File localShard = new File("/tmp/downloadedShard");
      fileSystem.copyToLocalFile(fileStatus.getPath(), new Path(localShardZip.getAbsolutePath()));

      FileUtil.unzip(localShardZip, localShard);
      localShardZip.delete();

      IndexReader indexReader = IndexReader.open(localShard);
      indexDocCount += indexReader.maxDoc();
      FileUtil.deleteFolder(localShard);
    }
    assertEquals(_documentCount, indexDocCount);
  }

  private void writeDocumentRecordSequenceFile(JobConf jobConf, Path inputPath, int count) throws IOException {
    ITestDocumentRecordGenerator documentRecordGenerator = new ITestDocumentRecordGenerator() {
      public TestDocumentRecord generate(int i) {
        LongWritable key = new LongWritable(i);
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new Text("field1"), new Text("value " + i));
        mapWritable.put(new Text("field2"), new BooleanWritable((i % 2) == 1));
        return new TestDocumentRecord(key, mapWritable);
      }
    };
    HadoopTestUtil.writeSequenceFile(jobConf, inputPath, documentRecordGenerator, count);
  }
}
