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
package net.sf.katta;

import junit.framework.TestCase;

import org.apache.hadoop.io.WritableComparator;

public class GeneralPerformanceTest extends TestCase {

  public void testStringComparison() throws Exception {

    int count = 0;
    final long start = System.currentTimeMillis();
    for (int i = 0; i < 1000000; i++) {
      final String s1 = "" + i;
      final String s2 = "" + (i * 2);
      final byte[] b1 = s1.getBytes();
      final byte[] b2 = s2.getBytes();
      // if (s1.compareTo(s2)==1) {
      if (WritableComparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length) == 1) {
        count++;
      }

    }
    System.out.println("took: " + (System.currentTimeMillis() - start));

    // WritableComparator

  }
}
