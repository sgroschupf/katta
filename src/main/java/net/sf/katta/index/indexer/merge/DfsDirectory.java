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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * A read-only Lucene {@link Directory} which is able to read an index out of
 * the hdfs.
 * 
 * The class is inspired by nutch's org.apache.nutch.indexer.FsDirectory which
 * by the way supports writing too.
 * 
 */
public class DfsDirectory extends Directory {

  protected FileSystem _fileSystem;
  private Path _path;
  private int _ioFileBufferSize;

  public DfsDirectory(FileSystem fs, Path path, int ioFileBufferSize) throws IOException {
    _fileSystem = fs;
    _path = path;
    _ioFileBufferSize = ioFileBufferSize;
    if (!fs.getFileStatus(path).isDir()) {
      throw new IllegalStateException(path + " is not a directory");
    }
  }

  public String[] list() throws IOException {
    FileStatus[] fileStatuses = _fileSystem.listStatus(_path);
    String[] ret = new String[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      FileStatus fileStatuse = fileStatuses[i];
      Path path = fileStatuse.getPath();
      ret[i] = path.getName();
    }
    return ret;
  }

  public boolean fileExists(String name) throws IOException {
    return _fileSystem.exists(new Path(_path, name));
  }

  public long fileModified(String name) {
    throw new UnsupportedOperationException();
  }

  public void touchFile(String name) {
    throw new UnsupportedOperationException();
  }

  public long fileLength(String name) throws IOException {
    return _fileSystem.getFileStatus(new Path(_path, name)).getLen();
  }

  public void deleteFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void renameFile(String from, String to) throws IOException {
    throw new UnsupportedOperationException();
  }

  public IndexOutput createOutput(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  public IndexInput openInput(String name) throws IOException {
    return new DfsIndexInput(new Path(_path, name), _ioFileBufferSize);
  }

  public synchronized void close() throws IOException {
    // nothing todo
  }

  public String toString() {
    return getClass().getName() + "@" + _path;
  }

  private class DfsIndexInput extends BufferedIndexInput {

    private class Descriptor {
      public FSDataInputStream _fsInputStream;
      public long _position; // cache of in.getPos()

      public Descriptor(Path file, int ioFileBufferSize) throws IOException {
        _fsInputStream = _fileSystem.open(file, ioFileBufferSize);
      }
    }

    private final Descriptor _descriptor;
    private final long _length;
    private boolean _isClone;

    public DfsIndexInput(Path path, int ioFileBufferSize) throws IOException {
      _descriptor = new Descriptor(path, ioFileBufferSize);
      _length = _fileSystem.getFileStatus(path).getLen();
    }

    protected void readInternal(byte[] b, int offset, int len) throws IOException {
      synchronized (_descriptor) {
        long position = getFilePointer();
        if (position != _descriptor._position) {
          _descriptor._fsInputStream.seek(position);
          _descriptor._position = position;
        }
        int total = 0;
        do {
          int i = _descriptor._fsInputStream.read(b, offset + total, len - total);
          if (i == -1)
            throw new IOException("read past EOF");
          _descriptor._position += i;
          total += i;
        } while (total < len);
      }
    }

    public void close() throws IOException {
      if (!_isClone) {
        _descriptor._fsInputStream.close();
      }
    }

    protected void seekInternal(long position) {
      // handled in readInternal()
    }

    public long length() {
      return _length;
    }

    protected void finalize() throws IOException {
      close(); // close the file
    }

    public Object clone() {
      DfsIndexInput clone = (DfsIndexInput) super.clone();
      clone._isClone = true;
      return clone;
    }
  }

}
