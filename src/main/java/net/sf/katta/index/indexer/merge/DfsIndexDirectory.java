package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import net.sf.katta.util.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class DfsIndexDirectory extends Directory {

  private FileSystem _fileSystem;
  private Path _workingFolderPath;


  public DfsIndexDirectory(FileSystem fileSystem, Path zipFile, Path workingFolderPath) throws IOException {
    _fileSystem = fileSystem;
    _workingFolderPath = workingFolderPath;
    if (zipFile != null && _fileSystem.exists(zipFile) && zipFile.getName().endsWith(".zip")) {
      decompress(zipFile, _workingFolderPath);
    }

  }

  public String[] list() throws IOException {
    FileStatus[] fileStatuses = _fileSystem.listStatus(_workingFolderPath);
    String[] ret = new String[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      FileStatus fileStatuse = fileStatuses[i];
      Path path = fileStatuse.getPath();
      ret[i] = path.toString();
    }
    return ret;
  }

  public boolean fileExists(String s) throws IOException {
    return _fileSystem.exists(new Path(_workingFolderPath, s));
  }

  public long fileModified(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void touchFile(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void deleteFile(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void renameFile(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  public long fileLength(String s) throws IOException {
    long len = 0;
    if (fileExists(s)) {
      len = _fileSystem.getFileStatus(new Path(_workingFolderPath, s)).getLen();
    }
    return len;
  }

  public IndexOutput createOutput(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public IndexInput openInput(String s) throws IOException {
    return new DfsIndexInput(_fileSystem, new Path(_workingFolderPath, s), 4096);
  }

  public void close() throws IOException {
    _fileSystem.close();
  }

  private void decompress(final Path source, final Path target) {
    try {
      FSDataInputStream dfsInputStream = _fileSystem.open(source);
      _fileSystem.mkdirs(target);
      final ZipInputStream zipInputStream = new ZipInputStream(dfsInputStream);
      ZipEntry entry;

      while ((entry = zipInputStream.getNextEntry()) != null) {
        final String entryPath = entry.getName();
        final int indexOf = entryPath.indexOf("/");
        final String cleanUpPath = entryPath.substring(indexOf + 1, entryPath.length());
        Path path = target;
        if (!cleanUpPath.equals("")) {
          path = new Path(target, cleanUpPath);
        }
        Logger.info("Extracting: " + entry + " to " + path);
        if (entry.isDirectory()) {
          _fileSystem.mkdirs(path);
        } else {
          int count;
          final byte data[] = new byte[4096];
          FSDataOutputStream fsDataOutputStream = _fileSystem.create(path);
          while ((count = zipInputStream.read(data, 0, 4096)) != -1) {
            fsDataOutputStream.write(data, 0, count);
          }
          fsDataOutputStream.flush();
          fsDataOutputStream.close();
        }
      }
      zipInputStream.close();

    } catch (final Exception e) {
      throw new RuntimeException("unable to expand upgrade files", e);
    }
  }

}
