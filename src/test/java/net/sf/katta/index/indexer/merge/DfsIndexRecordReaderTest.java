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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import net.sf.katta.testutil.ExtendedTestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class DfsIndexRecordReaderTest extends ExtendedTestCase {

  private File _file = createFile("_temporary/jobId");

  public void testNext() throws IOException, URISyntaxException {
    JobConf jobConf = new JobConf();
    Path out = new Path(_file.getAbsolutePath());
    FileOutputFormat.setOutputPath(jobConf, out);

    Mockery mockery = new Mockery();
    Path path = new Path("src/test/testIndexB/aIndex.zip");
    FileSystem fileSystem = FileSystem.get(jobConf);
    long len = fileSystem.getFileStatus(path).getLen();
    FileSplit fileSplit = new FileSplit(path, 0, len, (String[]) null);
    final IDocumentDuplicateInformation duplicateInformation = mockery.mock(IDocumentDuplicateInformation.class);

    DfsIndexRecordReader reader = new DfsIndexRecordReader(jobConf, fileSplit, duplicateInformation);

    mockery.checking(new Expectations() {
      {
        atLeast(1).of(duplicateInformation).getKeyField();
        will(returnValue("foo"));
        atLeast(1).of(duplicateInformation).getSortField();
        will(returnValue("foo"));
      }
    });

    Text text = reader.createKey();
    DocumentInformation information = reader.createValue();
    reader.next(text, information);

    assertEquals("bar", text.toString());
    assertEquals("bar", information.getSortValue().toString());
    assertEquals(0, information.getDocId().get());
    assertEquals(new File(out.toString(), ".indexes/" + path.getName() + "-" + MD5Hash.digest(path.toString())
        + "-uncompress").getAbsolutePath(), new File(new URI(information.getIndexPath().toString())).getAbsolutePath());

    mockery.assertIsSatisfied();
  }
}
