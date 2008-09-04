/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import net.sf.katta.testutil.TestResources;
import net.sf.katta.testutil.TestUtil;

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
    Directory directory = new DfsIndexDirectory(fileSystem, new Path(TestResources.SHARD1.getAbsolutePath()), new Path(
        _file.getAbsolutePath()));
    assertTrue(directory.fileExists("segments.gen"));
  }

  public void testListFiles() throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = FileSystem.get(configuration);
    Directory directory = new DfsIndexDirectory(fileSystem, new Path(TestResources.SHARD1.getAbsolutePath()), new Path(
        _file.getAbsolutePath()));
    String[] strings = directory.list();
    List<String> list = Arrays.asList(strings);
    assertEquals(3, list.size());
  }
}
