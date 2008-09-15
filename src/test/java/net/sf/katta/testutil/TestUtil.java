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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

public class TestUtil {

  public static final boolean deleteDirectory(File directory) {
    if (!directory.exists()) {
      return true;
    }
    File[] files = directory.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        if (!deleteDirectory(file)) {
          return false;
        }
      } else {
        if (!file.delete()) {
          return false;
        }
      }
    }
    return directory.delete();
  }

  public static void copy(File sourceFile, File targetFile) {
    targetFile.getParentFile().mkdirs();
    try {
      InputStream in = new FileInputStream(sourceFile);
      OutputStream out = new FileOutputStream(targetFile);
      copy(in, out, true);
    } catch (IOException e) {
      throw new RuntimeException("could not copy " + sourceFile + " to " + targetFile, e);
    }
  }

  public static void copy(InputStream inputStream, OutputStream outputStream, boolean close) throws IOException {
    byte[] buf = new byte[1024];
    int len;
    while ((len = inputStream.read(buf)) > 0) {
      outputStream.write(buf, 0, len);
    }
    if (close) {
      inputStream.close();
      outputStream.close();
    }
  }

  public static Path newTestFolder(String name) {
    return new Path("build/tests/" + name);
  }

}
