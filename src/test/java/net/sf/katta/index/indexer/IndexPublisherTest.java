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
package net.sf.katta.index.indexer;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.util.IndexConfiguration;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;

public class IndexPublisherTest extends TestCase {

  private final File _folder = new File(System.getProperty("java.io.tmpdir"), IndexPublisherTest.class.getName());
  private File _idx;

  @Override
  protected void setUp() throws Exception {
    assertTrue(_folder.mkdir());
    final File file = new File(_folder, "input");
    assertTrue(file.mkdir());
    _idx = new File(file, "idx");
    assertTrue(_idx.createNewFile());
  }

  @Override
  protected void tearDown() throws Exception {
    assertTrue(FileUtil.fullyDelete(_folder));
  }

  public void testPublish() throws Exception {
    // copy file: /folder/input/shard
    final IIndexPublisher publisher = new IndexUploader();
    final JobConf jobConf = new JobConf();
    jobConf
        .set(IndexConfiguration.INDEX_UPLOAD_PATH, "file://" + _folder.getAbsolutePath() + File.separator + "output");
    publisher.configure(jobConf);

    final File out = new File(_folder, "output");
    assertEquals(1, out.listFiles().length);
    final File[] indexes = out.listFiles();
    assertEquals(1, indexes.length);
    assertEquals("indexes", indexes[0].getName());

    publisher.publish(new File(_folder, "input").getAbsolutePath());
    assertEquals(1, indexes[0].listFiles().length);

    // index
    final File[] part = indexes[0].listFiles();
    assertEquals(1, part.length);

    final File[] outputFolders = part[0].listFiles();
    assertEquals(2, outputFolders.length);
    final List<File> asList = Arrays.asList(outputFolders);
    assertTrue(asList.contains(new File(part[0], "idx")));
    assertTrue(asList.contains(new File(part[0], ".idx.crc")));
  }
}
