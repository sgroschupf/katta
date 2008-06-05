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
package net.sf.katta.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Logger {

  private static boolean DEBUG = false;
  private static boolean ERROR = false;
  private static boolean INFO = false;
  private static boolean WARN = false;

  static {
    String logLevel = System.getProperty("KATTA_LOG_LEVEL");
    if (logLevel == null) {
      logLevel = "INFO";
    }
    if (logLevel.equalsIgnoreCase("DEBUG")) {
      DEBUG = true;
      ERROR = true;
      INFO = true;
      WARN = true;
    } else if (logLevel.equalsIgnoreCase("ERROR")) {
      ERROR = true;
    } else if (logLevel.equalsIgnoreCase("INFO")) {
      INFO = true;
      ERROR = true;
      WARN = true;
    } else if (logLevel.equalsIgnoreCase("WARN")) {
      WARN = true;
      ERROR = true;
    }
  }

  public static void info(final String string, final Throwable t) {
    if (INFO) {
      System.out.println(string + " \t" + stringifyException(t));
    }
  }

  private static String stringifyException(final Throwable t) {
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    t.printStackTrace(printWriter);
    printWriter.close();
    return stringWriter.toString();
  }

  public static void warn(final String string, final Throwable t) {
    if (WARN) {
      System.err.println(string + " \t" + stringifyException(t));
    }
  }

  public static void info(final String string) {
    if (INFO) {
      System.out.println(string);
    }
  }

  public static void debug(final String string) {
    if (DEBUG) {
      System.out.println(string);
    }
  }

  public static void warn(final String string) {
    if (WARN) {
      System.out.println(string);
    }
  }

  public static void error(final String string, final Throwable t) {
    if (ERROR) {
      System.err.println(string + " \t" + stringifyException(t));
    }
  }

  public static void error(final String string) {
    if (ERROR) {
      System.err.println(string);
    }
  }

  public static boolean isDebug() {
    return DEBUG;
  }

  public static boolean isError() {
    return ERROR;
  }

  public static boolean isInfo() {
    return INFO;
  }

  public static boolean isWarn() {
    return WARN;
  }
}
