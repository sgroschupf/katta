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
package net.sf.katta.node;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class DocumentFrequencyWritableTest extends TestCase {

  public void testAddNumDocsMultiThreading() throws InterruptedException {
    final DocumentFrequencyWritable writable = new DocumentFrequencyWritable();

    runThreads(10, writable, new Runnable() {
      @Override
      public void run() {
        for (int j = 0; j < 100000; j++) {
          writable.addNumDocs(1);
        }
      }
    });

    assertEquals(10 * 100000, writable.getNumDocs());
  }

  public void testAddFrequencies() throws InterruptedException {
    final DocumentFrequencyWritable writable = new DocumentFrequencyWritable();
    runThreads(10, writable, new Runnable() {
      @Override
      public void run() {
        for (int j = 0; j < 10000; j++) {
          writable.put("field", "term", 1);
        }
      }
    });

    assertEquals(10 * 10000, writable.get("field", "term").intValue());
  }

  private void runThreads(int numberOfThreads, final DocumentFrequencyWritable writable, Runnable runnable) throws InterruptedException {
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < numberOfThreads; i++) {
      threads.add(new Thread(runnable));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }
}
