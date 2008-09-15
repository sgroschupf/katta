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

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileUtil;

public class ZipServiceTest extends TestCase {

  private final String tmpString = System.getProperty("java.io.tmpdir");
  private final File _folder = new File(tmpString, ZipServiceTest.class.getName());

  @Override
  protected void setUp() throws Exception {
    assertTrue(_folder.mkdir());
  }

  @Override
  protected void tearDown() throws Exception {
    assertTrue(FileUtil.fullyDelete(_folder));
  }

  public void testZipFolder() throws Exception {

    final File folderToZip = new File(_folder, "foldertoZip");
    assertTrue(folderToZip.mkdir());
    final File foo = new File(folderToZip, "foo");
    assertTrue(foo.mkdir());
    final File bar = new File(foo, "bar");
    assertTrue(bar.mkdir());
    final File foobar = new File(bar, "foobar.txt");
    assertTrue(foobar.createNewFile());
    final IZipService zipService = new ZipService();
    final File outputFile = new File(_folder, "zipFile.zip");
    zipService.zipFolder(folderToZip, outputFile);
    assertTrue(outputFile.exists());

  }
}
