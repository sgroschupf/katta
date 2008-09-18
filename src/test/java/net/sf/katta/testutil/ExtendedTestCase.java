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
package net.sf.katta.testutil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import net.sf.katta.util.FileUtil;

import org.apache.hadoop.fs.Path;

/**
 * JUnit 3 test case with JUnit 4 like {@link #beforeClass()} and
 * {@link #afterClass()} methods.
 * 
 * Inspired by
 * <i>http://blog.jakubpawlowicz.com/2006/08/31/beforeclass-and-afterclass
 * -with-junit-3</i>
 * 
 * Additionally it provides some util methods f.e. for creating temporary
 * folders.
 * 
 */
public abstract class ExtendedTestCase extends TestCase {

  private static int _testMethodsLeft = 0;

  private Map<String, File> _testName2rootFolder = new HashMap<String, File>();

  private int countTestMethods() {
    int testMethodCount = 0;
    for (Method method : getClass().getMethods()) {
      if (method.getName().startsWith("test")) {
        testMethodCount++;
      }
    }
    return testMethodCount;
  }

  @Override
  protected final void setUp() throws Exception {
    if (_testMethodsLeft == 0) {
      _testMethodsLeft = countTestMethods();
      System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + " ~~~~~~~~~~~~~~~");
      beforeClass();
    }
    System.out.println("~~~~~~~~~~~~~~~ " + getClass().getName() + "#" + getName() + "() ~~~~~~~~~~~~~~~");
    onSetUp();
  }

  @Override
  protected final void tearDown() throws Exception {
    onTearDown();
    if (--_testMethodsLeft == 0) {
      System.out.println("~~~~~~~~~~~~~~~ FINISHING " + getClass().getName() + " ~~~~~~~~~~~~~~~");
      afterClass();
    }
    Collection<File> files = _testName2rootFolder.values();
    for (File file : files) {
      FileUtil.deleteFolder(file);
    }
  }

  protected void onTearDown() throws Exception {
    // subclasses may override
  }

  protected void onSetUp() throws Exception {
    // subclasses may override
  }

  protected void beforeClass() throws Exception {
    // subclasses may override
  }

  protected void afterClass() throws Exception {
    // subclasses may override
  }

  /**
   * Creates a file in a temporary empty folder. On tearing the test down this
   * folder will be deleted.
   */
  protected File createFile(String name) {
    File rootFolder = getTmpRootFolder();
    return new File(rootFolder, name);
  }

  /**
   * Creates a path in a temporary empty folder. On tearing the test down this
   * folder will be deleted.
   */
  protected Path createPath(String name) {
    File rootFolder = getTmpRootFolder();
    return new Path(rootFolder.getAbsolutePath(), name);
  }

  protected File getTmpRootFolder() {
    String testName = getName();
    if (testName == null) {
      testName = this.getClass().getSimpleName();
    }
    File rootFolder = _testName2rootFolder.get(testName);
    if (rootFolder == null) {
      try {
        rootFolder = File.createTempFile(testName + "_", ".tmp");
      } catch (IOException e) {
        throw new RuntimeException("could not create tmp file", e);
      }
      rootFolder.delete();
      rootFolder.mkdirs();
      _testName2rootFolder.put(testName, rootFolder);
    }

    return rootFolder;
  }
}
