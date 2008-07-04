package net.sf.katta.index.indexer.merge;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.util.TestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;

public class DfsIndexDirectoryTest extends TestCase {

  private File _file = new File(System.getProperty("java.io.tmpdir"), DfsIndexDirectory.class.getName());

  protected void setUp() throws Exception {
    assertTrue(_file.mkdir());
  }

  protected void tearDown() throws Exception {
    assertTrue(TestUtil.deleteDirectory(_file));
  }

  public void testFileExists() throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);
    Directory directory = new DfsIndexDirectory(fileSystem, new Path("src/test/testIndexB/aIndex.zip"), new Path(_file.getAbsolutePath()));
    assertTrue(directory.fileExists("segments.gen"));
  }

  public void testListFiles() throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);
    Directory directory = new DfsIndexDirectory(fileSystem, new Path("src/test/testIndexB/aIndex.zip"), new Path(_file.getAbsolutePath()));
    String[] strings = directory.list();
    List<String> list = Arrays.asList(strings);
    assertEquals(3, list.size());
  }
}

