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
package net.sf.katta.util;

import java.util.Map;

public class StringUtil {

  /**
   * Usage:<br>
   * String callingMethod =
   * StringUtil.getCallingMethod(Thread.currentThread().getStackTrace());
   */
  public static String getCallingMethod(StackTraceElement[] stackTrace) {
    int indexOfCallingMethod = 2;
    if (stackTrace[0].toString().endsWith("(Native Method)")) {
      // on some jvm the native methods get printed on some not
      indexOfCallingMethod = 3;
    }
    return stackTrace[indexOfCallingMethod].toString();
  }

  public static String formatTimeDuration(long timeDuration) {
    StringBuilder builder = new StringBuilder();
    long hours = timeDuration / (60 * 60 * 1000);
    long rem = (timeDuration % (60 * 60 * 1000));
    long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    long seconds = rem / 1000;

    if (hours != 0) {
      builder.append(hours);
      builder.append(" hrs, ");
    }
    if (minutes != 0) {
      builder.append(minutes);
      builder.append(" mins, ");
    }
    // return "0sec if no difference
    builder.append(seconds);
    builder.append(" sec");
    return builder.toString();
  }

  /**
   * 
   * @param string
   * @param length
   * @return the given path + as many whitespace that the given string reaches
   *         the given length
   */
  public static String fillWithWhiteSpace(String string, int length) {
    int neededWhiteSpace = length - string.length();
    if (neededWhiteSpace > 0) {
      StringBuilder builder = new StringBuilder(string);
      for (int i = 0; i < neededWhiteSpace; i++) {
        builder.append(" ");
      }
      return builder.toString();
    }
    return string;
  }

  /**
   * Gets all thread stack traces.
   * 
   * @return string of all thread stack traces
   */
  public static String getThreadDump() {
    StringBuilder sb = new StringBuilder();
    Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
    for (Thread thread : stacks.keySet()) {
      sb.append(thread.toString()).append('\n');
      for (StackTraceElement stackTraceElement : thread.getStackTrace()) {
        sb.append("\tat ").append(stackTraceElement.toString()).append('\n');
      }
      sb.append('\n');
    }
    return sb.toString();
  }

}
