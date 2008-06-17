/**
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
package org.apache.hadoop.contrib.dlucene;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * Utility class for static methods.
 */
public class Utils {

  /** Log file. */
  private static final Log LOG = LogFactory
      .getLog("org.apache.hadoop.dlucene.Utils");

  /**
   * Check argument(s) are not null.
   *
   * @param object argument to check for null
   */
  public static void checkArgs(Object object) {
    if (object == null) {
      throw new IllegalArgumentException("Argument is null");
    }
  }

  /**
   * Check argument(s) are not null.
   *
   * @param object1 argument to check for null
   * @param object2 argument to check for null
   */
  public static void checkArgs(Object object1, Object object2) {
    if (object1 == null || object2 == null) {
      throw new IllegalArgumentException("Argument is null");
    }
  }

  /**
   * Check argument(s) are not null.
   *
   * @param object1 argument to check for null
   * @param object2 argument to check for null
   * @param object3 argument to check for null
   */
  public static void checkArgs(Object object1, Object object2,
      final Object object3) {
    if (object1 == null || object2 == null || object3 == null) {
      throw new IllegalArgumentException("Argument is null");
    }
  }

  /**
   * Check this is a directory, and it is possible to read and write to it.
   *
   * @param directory the directory
   * @throws IOException thrown if error checking the directory
   */
  public static void checkDirectoryIsReadableWritable(File directory)
      throws IOException {
    try {
      DiskChecker.checkDir(directory);
    } catch (DiskErrorException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException("Invalid directory in dlucene.data.dir: "
          + e.getMessage());
    }
  }
  
  /**
   * Delete a directory and its contents safely
   * 
   * @param dir the directory
   * @return
   */
  public static boolean deleteDir(File dir) {
      // to see if this directory is actually a symbolic link to a directory,
      // we want to get its canonical path - that is, we follow the link to
      // the file it's actually linked to
      File candir;
      try {
          candir = dir.getCanonicalFile();
      } catch (IOException e) {
          return false;
      }

      // a symbolic link has a different canonical path than its actual path,
      // unless it's a link to itself
      if (!candir.equals(dir.getAbsoluteFile())) {
          // this file is a symbolic link, and there's no reason for us to
          // follow it, because then we might be deleting something outside of
          // the directory we were told to delete
          return false;
      }

      // now we go through all of the files and subdirectories in the
      // directory and delete them one by one
      File[] files = candir.listFiles();
      if (files != null) {
          for (int i = 0; i < files.length; i++) {
              File file = files[i];

              // in case this directory is actually a symbolic link, or it's
              // empty, we want to try to delete the link before we try
              // anything
              boolean deleted = file.delete();
              if (!deleted) {
                  // deleting the file failed, so maybe it's a non-empty
                  // directory
                  if (file.isDirectory()) deleteDir(file);

                  // otherwise, there's nothing else we can do
              }
          }
      }

      // now that we tried to clear the directory out, we can try to delete it
      // again
      return dir.delete();  
  }
}
