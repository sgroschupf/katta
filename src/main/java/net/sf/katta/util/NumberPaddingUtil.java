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

import java.text.DecimalFormat;
import java.util.TreeSet;

public class NumberPaddingUtil {

  private static final DecimalFormat FORMAT = new DecimalFormat("0000000000.####");

  /**
   * @param number
   * @return the padded number as string
   */
  public static String padding(final double number) {
    return FORMAT.format(number);
  }

  public static void main(final String[] args) {
    final TreeSet<String> set = new TreeSet<String>();
    set.add(NumberPaddingUtil.padding(1));
    set.add(NumberPaddingUtil.padding(1.01));
    set.add(NumberPaddingUtil.padding(1.10));
    set.add(NumberPaddingUtil.padding(1.123));
    set.add(NumberPaddingUtil.padding(10.123));
    set.add(NumberPaddingUtil.padding(2));
    set.add(NumberPaddingUtil.padding(2.3));
    set.add(NumberPaddingUtil.padding(9));
    for (final Object object : set) {
      System.out.println(object);
    }
  }
}
