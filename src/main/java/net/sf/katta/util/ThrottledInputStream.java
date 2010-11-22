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

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 * An {@link InputStream} which throttles the amount of bytes which is read from
 * the underlying {@link InputStream} in a given time frame.
 * 
 * <p>
 * Usage Example: <br>
 * <i>//creates an throttled input stream which reads 1024 bytes/sec from the
 * underlying input stream at the most </i> <br>
 * <code>
 * ThrottledInputStream throttledInputStream = new ThrottledInputStream(otherIputStream, new ThrottleSemaphore(1024));
 * </code><br>
 * 
 * <p>
 * Usage over multiple {@link InputStream}s: <br>
 * <i>//throttle the read of multiple input streams at the rate of 1024
 * bytes/sec </i> <br>
 * <code>
 * ThrottleSemaphore semaphore = new ThrottleSemaphore(1024);<br>
 * ThrottledInputStream throttledInputStream1 = new ThrottledInputStream(otherIputStream1, semaphore);<br>
 * ThrottledInputStream throttledInputStream2 = new ThrottledInputStream(otherIputStream2, semaphore);<br>
 * ...
 * </code><br>
 */
public class ThrottledInputStream extends InputStream implements PositionedReadable, Seekable {

  private final InputStream _inputStream;
  private final ThrottleSemaphore _semaphore;

  public ThrottledInputStream(InputStream inputStream, ThrottleSemaphore semaphore) {
    _inputStream = inputStream;
    _semaphore = semaphore;
  }

  @Override
  public int read() throws IOException {
    _semaphore.aquireBytes(1);
    return _inputStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    len = _semaphore.aquireBytes(len);
    return _inputStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    _inputStream.close();
  }

  @Override
  public int read(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
    return ((PositionedReadable) _inputStream).read(arg0, arg1, arg2, arg3);
  }

  @Override
  public void readFully(long arg0, byte[] arg1) throws IOException {
    ((PositionedReadable) _inputStream).readFully(arg0, arg1);
  }

  @Override
  public void readFully(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
    ((PositionedReadable) _inputStream).readFully(arg0, arg1, arg2, arg3);
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) _inputStream).getPos();
  }

  @Override
  public void seek(long arg0) throws IOException {
    ((Seekable) _inputStream).seek(arg0);
  }

  @Override
  public boolean seekToNewSource(long arg0) throws IOException {
    return ((Seekable) _inputStream).seekToNewSource(arg0);
  }

  /**
   * This semaphore maintains the permitted bytes in a given timeframe. Each
   * {@link #aquireBytes(int)} blocks if necessary until at least one byte can
   * be acquired.
   * 
   * <p>
   * The time unit is bytes/second whereas the window of one second is splitted
   * into smaller windows to allow more steadied operations.
   * 
   * <p>
   * This class is thread safe and one instance can be used by multiple threads/
   * {@link ThrottledInputStream}s. (But it might not be fair to the different
   * treads)
   */
  public static class ThrottleSemaphore {

    private static final int SECOND = 1000;
    private final int _maxBytesPerWindow;
    private final int _windowsPerSecond;
    private volatile int _remainingBytesInCurrentWindow;
    private volatile long _nextWindowStartTime;

    public ThrottleSemaphore(float bytesPerSecond) {
      this(bytesPerSecond, 10);
    }

    public ThrottleSemaphore(float bytesPerSecond, int windowsPerSecond) {
      checkForGreaterZero((int) bytesPerSecond);
      checkForGreaterZero(windowsPerSecond);
      _windowsPerSecond = windowsPerSecond;
      _maxBytesPerWindow = (int) (bytesPerSecond / windowsPerSecond);
    }

    private void checkForGreaterZero(int value) {
      if (value <= 0) {
        throw new IllegalArgumentException("argument must be greater the 0 but is " + value);
      }
    }

    public synchronized int aquireBytes(int desired) throws IOException {
      try {
        waitForAllowedBytes();
        int aquiredBytes = Math.min(desired, _remainingBytesInCurrentWindow);
        _remainingBytesInCurrentWindow -= aquiredBytes;
        return aquiredBytes;
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }

    private void waitForAllowedBytes() throws InterruptedException {
      updateWindow();
      while (_remainingBytesInCurrentWindow <= 0) {
        updateWindow();
        Thread.sleep(_nextWindowStartTime - System.currentTimeMillis());
      }
    }

    private final void updateWindow() {
      long now = System.currentTimeMillis();
      while (now >= _nextWindowStartTime) {
        if (now >= _nextWindowStartTime + SECOND) {
          _nextWindowStartTime = now + SECOND / _windowsPerSecond;
          _remainingBytesInCurrentWindow = _maxBytesPerWindow;
        }
        _nextWindowStartTime += SECOND / _windowsPerSecond;
        _remainingBytesInCurrentWindow += _maxBytesPerWindow;
      }
    }
  }

}
