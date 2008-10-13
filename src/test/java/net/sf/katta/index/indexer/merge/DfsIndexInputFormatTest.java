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
import java.util.ArrayList;

import net.sf.katta.testutil.ExtendedTestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.jmock.Expectations;
import org.jmock.Mockery;

public class DfsIndexInputFormatTest extends ExtendedTestCase {

  private File _file = createFile(getClass().getSimpleName());

  public void testInputFormat() throws IOException {
    DfsIndexInputFormat indexInputFormat = new DfsIndexInputFormat();
    JobConf jobConf = new JobConf();
    FileInputFormat.setInputPaths(jobConf, new Path("src/test"));
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
    jobConf.set(DfsIndexInputFormat.DOCUMENT_INFORMATION, ConfigurableDocumentDuplicateInformation.class.getName());
    jobConf.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_DOCUMENT_FIELDS, "foo");
    jobConf.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_KEY_FIELD, "foo");
    jobConf.set(ConfigurableDocumentDuplicateInformation.CONF_KEY_SORT_FIELD, "foo");

    FileInputFormat.setInputPaths(jobConf, new Path("src/test"));
    FileOutputFormat.setOutputPath(jobConf, new Path(_file.getAbsolutePath()));

    InputSplit[] splits = indexInputFormat.getSplits(jobConf, 0);
    Mockery mockery = new Mockery();
    final Reporter reporter = mockery.mock(Reporter.class);

    mockery.checking(new Expectations() {
      {
        atLeast(1).of(reporter).setStatus(with(any(String.class)));
      }
    });

    RecordReader reader = indexInputFormat.getRecordReader(splits[0], jobConf, reporter);
    assertNotNull(reader);
    assertTrue(DfsIndexRecordReader.class.isAssignableFrom(reader.getClass()));

    mockery.assertIsSatisfied();

  }
}
