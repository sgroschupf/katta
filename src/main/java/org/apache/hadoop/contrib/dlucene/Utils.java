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
}
