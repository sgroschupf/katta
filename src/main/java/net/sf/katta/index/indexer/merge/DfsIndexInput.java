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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;

public class DfsIndexInput extends BufferedIndexInput {

  private final static Logger LOG = Logger.getLogger(DfsIndexInput.class);

  private long _len;
  private FileSystem _fileSystem;
  private FSDataInputStream _fsDataInputStream;

  public DfsIndexInput(FileSystem fileSystem, Path file, int ioFileBufferSize) throws IOException {
    // FIXME unsused parameter !!!
    _fileSystem = fileSystem;
    _len = _fileSystem.getFileStatus(file).getLen();
    _fsDataInputStream = _fileSystem.open(file);
  }

  protected void readInternal(byte[] b, int offset, int len) {
    try {
      _fsDataInputStream.read(b, offset, len);
    } catch (IOException e) {
      LOG.warn("can not read datas from inputstream", e);
    }
  }

  public void close() throws IOException {
    _fsDataInputStream.close();
  }

  protected void seekInternal(long position) throws IOException {
    _fsDataInputStream.seek(position);
  }

  public long length() {
    return _len;
  }

  protected void finalize() throws IOException {
    close();
  }

}
