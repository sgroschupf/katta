package net.sf.katta.index.indexer.merge;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;
import net.sf.katta.util.TestUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.document.Document;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class DfsIndexRecordReaderTest extends TestCase {

  private File _file = new File(System.getProperty("java.io.tmpdir"), DfsIndexRecordReaderTest.class.getName() + File.separator + "_temporary" + File.separator + "jobId");

  protected void setUp() throws Exception {
    assertTrue(_file.mkdirs());
  }

  protected void tearDown() throws Exception {
    assertTrue(TestUtil.deleteDirectory(_file));
  }

  public void testNext() throws IOException, URISyntaxException {
    JobConf jobConf = new JobConf();
    Path out = new Path(_file.getAbsolutePath());
    jobConf.setOutputPath(out);

    Mockery mockery = new Mockery();
    Path path = new Path("src/test/testIndexB/aIndex.zip");
    FileSystem fileSystem = FileSystem.get(jobConf);
    long len = fileSystem.getFileStatus(path).getLen();
    FileSplit fileSplit = new FileSplit(path, 0, len, jobConf);
    final IDocumentDuplicateInformation duplicateInformation = mockery.mock(IDocumentDuplicateInformation.class);

    DfsIndexRecordReader reader = new DfsIndexRecordReader(jobConf, fileSplit, duplicateInformation);


    mockery.checking(new Expectations() {
      {
        one(duplicateInformation).getKey(with(any(Document.class)));
        will(returnValue("foo"));
        one(duplicateInformation).getSortValue(with(any(Document.class)));
        will(returnValue("1"));
      }
    });

    Text text = reader.createKey();
    DocumentInformation information = reader.createValue();
    reader.next(text, information);

    assertEquals("foo", text.toString());
    assertEquals(0, information.getDocId().get());
    assertEquals(new File(out.getParent().getParent().toString(), ".indexes/" + path.getName() + "-uncompress").getAbsolutePath(), new File(new URI(information.getIndexPath().toString())).getAbsolutePath());
    assertEquals("1", information.getSortValue().toString());

    mockery.assertIsSatisfied();
  }
}
