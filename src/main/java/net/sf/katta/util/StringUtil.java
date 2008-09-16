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
}
