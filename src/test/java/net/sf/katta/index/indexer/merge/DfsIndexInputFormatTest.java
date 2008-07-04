package net.sf.katta.index.indexer.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import junit.framework.TestCase;
import net.sf.katta.util.TestUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.jmock.Mockery;

public class DfsIndexInputFormatTest extends TestCase {

  private File _file = new File(System.getProperty("java.io.tmpdir"), DfsIndexInputFormatTest.class.getName());

  protected void setUp() throws Exception {
    assertTrue(_file.mkdir());
  }

  protected void tearDown() throws Exception {
    assertTrue(TestUtil.deleteDirectory(_file));
  }

  public void testInputFormat() throws IOException {
    DfsIndexInputFormat indexInputFormat = new DfsIndexInputFormat();
    JobConf jobConf = new JobConf();
    jobConf.setInputPath(new Path("src/test"));
    InputSplit[] splits = indexInputFormat.getSplits(jobConf, 0);
    assertEquals(4, splits.length);
    ArrayList<String> strings = new ArrayList<String>();
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      Path path = fileSplit.getPath();
      strings.add(path.getName());
    }
    assertTrue(strings.contains("aIndex.zip"));
    assertTrue(strings.contains("bIndex.zip"));
    assertTrue(strings.contains("cIndex.zip"));
    assertTrue(strings.contains("dindex.zip"));
  }

  public void testGetRecordReader() throws IOException {
    DfsIndexInputFormat indexInputFormat = new DfsIndexInputFormat();
    JobConf jobConf = new JobConf();
    jobConf.set(DfsIndexInputFormat.DOCUMENT_INFORMATION, DummyDocumentDuplicateInformation.class.getName());
    jobConf.setInputPath(new Path("src/test"));
    jobConf.setOutputPath(new Path(_file.getAbsolutePath()));

    InputSplit[] splits = indexInputFormat.getSplits(jobConf, 0);
    Mockery mockery = new Mockery();
    Reporter reporter = mockery.mock(Reporter.class);
    RecordReader reader = indexInputFormat.getRecordReader(splits[0], jobConf, reporter);
    assertNotNull(reader);
    assertTrue(DfsIndexRecordReader.class.isAssignableFrom(reader.getClass()));

    mockery.assertIsSatisfied();

  }

}
