package net.sf.katta.index.indexer.merge;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;

public class DfsIndexInput extends BufferedIndexInput {

  private long _len;
  private FileSystem _fileSystem;
  private FSDataInputStream _fsDataInputStream;

  public DfsIndexInput(FileSystem fileSystem, Path file, int ioFileBufferSize) throws IOException {
    _fileSystem = fileSystem;
    _len = _fileSystem.getFileStatus(file).getLen();
    _fsDataInputStream = _fileSystem.open(file);
  }


  protected void readInternal(byte[] b, int offset, int len) {
    try {
      _fsDataInputStream.read(b, offset, len);
    } catch (IOException e) {
      e.printStackTrace();
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
