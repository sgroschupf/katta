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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class DfsIndexDirectory extends Directory {

  private final static Logger LOG = Logger.getLogger(DfsIndexDirectory.class);

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
    // _fileSystem.close();
  }

  private void decompress(final Path source, final Path target) throws IOException {
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
        LOG.info("Extracting: " + entry + " to " + path);
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
      LOG.error("can not open zip file", e);
      throw new IOException("unable to expand upgrade files " + e.getMessage());
    }
  }

}
