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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import net.sf.katta.AbstractTest;
import net.sf.katta.util.ThrottledInputStream.ThrottleSemaphore;

import org.junit.Test;

public class ThrottledInputStreamTest extends AbstractTest {

  private static final int READ_TIME = 2000;

  @Test(timeout = 10000)
  public void testThrottleRead() throws Exception {
    ReadThread readThread = checkThrottledRead(READ_TIME, 1024, false);
    printResult(readThread);
  }

  @Test(timeout = 10000)
  public void testThrottleReadWithBuffer() throws Exception {
    ReadThread readThread = checkThrottledRead(READ_TIME, 1024, true);
    printResult(readThread);
  }

  @Test(timeout = 10000)
  public void testThrottleReadFromMultipleInputStreams() throws Exception {
    long expectedReadTime = READ_TIME;
    int bytesPerSecond = 5000;
    ThrottleSemaphore semaphore = new ThrottleSemaphore(bytesPerSecond);
    ThrottledInputStream throttledInputStream1 = new ThrottledInputStream(new EndlessFastInputStream(), semaphore);
    ThrottledInputStream throttledInputStream2 = new ThrottledInputStream(new EndlessFastInputStream(), semaphore);

    ReadThread readThread1 = new ReadThread(throttledInputStream1, true);
    ReadThread readThread2 = new ReadThread(throttledInputStream2, true);
    readThread1.start();
    readThread2.start();
    readThread1.join(expectedReadTime);

    readThread1.interrupt();
    readThread2.interrupt();
    readThread1.join();
    readThread2.join();

    printResult(readThread1);
    printResult(readThread2);

    assertThat(readThread1.getReadTime(), almostEquals(expectedReadTime, 1000));
    assertThat(readThread2.getReadTime(), almostEquals(expectedReadTime, 1000));
    assertThat((readThread1.getReadBytes() + readThread2.getReadBytes()) / (expectedReadTime / 1000), almostEquals(
            bytesPerSecond, 700));
  }

  private ReadThread checkThrottledRead(long expectedReadTime, int bytesPerSecond, boolean readWithBuffer)
          throws InterruptedException {
    ThrottledInputStream throttledInputStream = new ThrottledInputStream(new EndlessFastInputStream(),
            new ThrottleSemaphore(bytesPerSecond));
    ReadThread readThread = new ReadThread(throttledInputStream, readWithBuffer);
    readThread.start();
    readThread.join(expectedReadTime);

    readThread.interrupt();
    readThread.join();

    assertThat(readThread.getReadTime(), almostEquals(expectedReadTime, 1000));
    assertThat(readThread.getReadBytes() / (readThread.getReadTime() / 1000), almostEquals(bytesPerSecond, 200));

    return readThread;
  }

  private void printResult(ReadThread readThread) {
    System.out.println("read " + readThread.getReadTime() + " ms");
    System.out.println("read " + readThread.getReadBytes() + " bytes");
    System.out.println("rate " + (readThread.getReadBytes() / (readThread.getReadTime() / 1000)) + " bytes/sec");
  }

  protected class EndlessFastInputStream extends InputStream {
    private Random _random = new Random(3);

    @Override
    public int read() throws IOException {
      return _random.nextInt(255);
    }
  }

  private class ReadThread extends Thread {

    private final InputStream _inputStream;
    private final boolean _readWithBuffer;
    private volatile boolean _stopped = false;
    private long _readTime;
    private long _totalReadBytes;

    public ReadThread(InputStream inputStream, boolean readWithBuffer) {
      _inputStream = inputStream;
      _readWithBuffer = readWithBuffer;
    }

    public long getReadTime() {
      return _readTime;
    }

    public long getReadBytes() {
      return _totalReadBytes;
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      try {
        byte[] buffer = new byte[4096];
        int readBytes;
        do {
          if (_readWithBuffer) {
            readBytes = _inputStream.read(buffer);
          } else {
            _inputStream.read();
            readBytes = 1;
          }
          _totalReadBytes += readBytes;
        } while (readBytes != -1 && !_stopped);
      } catch (IOException e) {
        fail("exception on read:" + e.getMessage());
      }
      _readTime = System.currentTimeMillis() - startTime;
    }

    @Override
    public void interrupt() {
      _stopped = true;
    }
  }

}
