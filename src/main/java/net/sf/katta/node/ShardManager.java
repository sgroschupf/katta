package net.sf.katta.node;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import net.sf.katta.util.FileUtil;
import net.sf.katta.util.KattaException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class ShardManager {

  protected final static Logger LOG = Logger.getLogger(ShardManager.class);
  private final File _shardsFolder;

  public ShardManager(File shardsFolder) {
    _shardsFolder = shardsFolder;
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

  public void uninstallSuperfluousShard(Collection<String> shardsToKeep) {
    String[] folderList = _shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
    if (folderList != null) {
      for (String shard : folderList) {
        if (!shardsToKeep.contains(shard)) {
          File localShard = getShardFolder(shard);
          LOG.info("delete local shard " + localShard.getAbsolutePath());
          FileUtil.deleteFolder(localShard);
        }
      }
    }
  }

  public Collection<String> getInstalledShards() {
    String[] folderList = _shardsFolder.list(FileUtil.VISIBLE_FILES_FILTER);
    return Arrays.asList(folderList);
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
        final FileSystem fileSystem = FileSystem.get(uri, new Configuration());
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

}
