package net.sf.katta.node;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

public class DocumentFrequenceWritableTest extends TestCase {

  public void testAddNumDocsMultiThreading() throws InterruptedException {
    final DocumentFrequenceWritable writable = new DocumentFrequenceWritable();

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
    final DocumentFrequenceWritable writable = new DocumentFrequenceWritable();
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

  private void runThreads(int numberOfThreads, final DocumentFrequenceWritable writable, Runnable runnable) throws InterruptedException {
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
