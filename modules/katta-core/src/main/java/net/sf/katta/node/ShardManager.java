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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;
import net.sf.katta.util.ThrottledInputStream;
import net.sf.katta.util.ThrottledInputStream.ThrottleSemaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class ShardManager {

  protected final static Logger LOG = Logger.getLogger(ShardManager.class);
  private final File _shardsFolder;
  private final ThrottleSemaphore _throttleSemaphore;

  public ShardManager(File shardsFolder) {
    this(shardsFolder, null);
  }

  public ShardManager(File shardsFolder, ThrottleSemaphore throttleSemaphore) {
    _shardsFolder = shardsFolder;
    _throttleSemaphore = throttleSemaphore;
    if (!_shardsFolder.exists()) {
      _shardsFolder.mkdirs();
    }
    if (!_shardsFolder.exists()) {
      throw new IllegalStateException("could not create local shard folder '" + _shardsFolder.getAbsolutePath() + "'");
    }
  }

  public File installShard(String shardName, String shardPath) throws Exception {
    File localShardFolder = getShardFolder(shardName);
    try {
      if (!localShardFolder.exists()) {
        installShard(shardName, shardPath, localShardFolder);
      }
      return localShardFolder;
    } catch (Exception e) {
      FileUtil.deleteFolder(localShardFolder);
      throw e;
    }
  }

  public void uninstallShard(String shardName) {
    File localShardFolder = getShardFolder(shardName);
    FileUtil.deleteFolder(localShardFolder);
  }

  public Collection<String> getInstalledShards() {
    String[] folderList = _shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
    if (folderList == null) {
      return Collections.EMPTY_LIST;
    }
    return Arrays.asList(folderList);
  }

  public File getShardsFolder() {
    return _shardsFolder;
  }

  public File getShardFolder(String shardName) {
    return new File(_shardsFolder, shardName);
  }

  /*
   * Loads a shard from the given URI. The uri is handled bye the hadoop file
   * system. So all hadoop support file systems can be used, like local hdfs s3
   * etc. In case the shard is compressed we also unzip the content. If the
   * system property katta.spool.zip.shards is true, the zip file is staged to
   * the local disk before being unzipped.
   */
  private void installShard(String shardName, String shardPath, File localShardFolder) throws KattaException {
    LOG.info("install shard '" + shardName + "' from " + shardPath);
    // TODO sg: to fix HADOOP-4422 we try to download the shard 5 times
    int maxTries = 5;
    for (int i = 0; i < maxTries; i++) {
      URI uri;
      try {
        uri = new URI(shardPath);
        FileSystem fileSystem = FileSystem.get(uri, new Configuration());
        if (_throttleSemaphore != null) {
          fileSystem = new ThrottledFileSystem(fileSystem, _throttleSemaphore);
        }
        final Path path = new Path(shardPath);
        boolean isZip = fileSystem.isFile(path) && shardPath.endsWith(".zip");

        File shardTmpFolder = new File(localShardFolder.getAbsolutePath() + "_tmp");
        try {
          FileUtil.deleteFolder(localShardFolder);
          FileUtil.deleteFolder(shardTmpFolder);

          if (isZip) {
            FileUtil.unzip(path, shardTmpFolder, fileSystem, System.getProperty("katta.spool.zip.shards", "false")
                    .equalsIgnoreCase("true"));
          } else {
            fileSystem.copyToLocalFile(path, new Path(shardTmpFolder.getAbsolutePath()));
          }
          shardTmpFolder.renameTo(localShardFolder);
        } finally {
          // Ensure that the tmp folder is deleted on an error
          FileUtil.deleteFolder(shardTmpFolder);
        }
        // Looks like we were successful.
        if (i > 0) {
          LOG.error("Loaded shard:" + shardPath);
        }
        return;
      } catch (final URISyntaxException e) {
        throw new KattaException("Can not parse uri for path: " + shardPath, e);
      } catch (final Exception e) {
        LOG.error(String.format("Error loading shard: %s (try %d of %d)", shardPath, i, maxTries), e);
        if (i >= maxTries - 1) {
          throw new KattaException("Can not load shard: " + shardPath, e);
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static class ThrottledFileSystem extends FileSystem {

    private final FileSystem _fileSystemDelegate;
    private final ThrottleSemaphore _throttleSemaphore;

    public ThrottledFileSystem(FileSystem fileSystem, ThrottleSemaphore throttleSemaphore) {
      _fileSystemDelegate = fileSystem;
      _throttleSemaphore = throttleSemaphore;
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
      return _fileSystemDelegate.append(arg0, arg1, arg2);
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
            Progressable arg6) throws IOException {
      return _fileSystemDelegate.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public boolean delete(Path arg0) throws IOException {
      return _fileSystemDelegate.delete(arg0);
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
      return _fileSystemDelegate.delete(arg0, arg1);
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
      return _fileSystemDelegate.getFileStatus(arg0);
    }

    @Override
    public URI getUri() {
      return _fileSystemDelegate.getUri();
    }

    @Override
    public Path getWorkingDirectory() {
      return _fileSystemDelegate.getWorkingDirectory();
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws IOException {
      return _fileSystemDelegate.listStatus(arg0);
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
      return _fileSystemDelegate.mkdirs(arg0, arg1);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
      ThrottledInputStream throttledInputStream = new ThrottledInputStream(_fileSystemDelegate.open(path, bufferSize),
              _throttleSemaphore);
      return new FSDataInputStream(throttledInputStream);
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
      return _fileSystemDelegate.rename(arg0, arg1);
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
      _fileSystemDelegate.setWorkingDirectory(arg0);
    }

    @Override
    public Configuration getConf() {
      return _fileSystemDelegate.getConf();
    }
  }
}
