package net.sf.katta.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;

public class FileUtil {

  private final static Logger LOG = Logger.getLogger(FileUtil.class);

  private static final int BUFFER = 4096;
  
 public static final FilenameFilter VISIBLE_FILES_FILTER= new FilenameFilter() {
    public boolean accept(final File dir, final String name) {
        return !name.startsWith(".");
    }
};

  public static void deleteFolder(File folder) {
    try {
      org.apache.hadoop.fs.FileUtil.fullyDelete(folder);
    } catch (IOException e) {
      throw new RuntimeException("could not delete folder '" + folder + "'");
    }
  }

  /**
   * Simply unzips the content from the source zip to the target folder. The
   * first level folder of the zip content is removed.
   */
  public static void unzip(final File sourceZip, final File targetFolder) {
    try {
      targetFolder.mkdirs();
      BufferedOutputStream dest = null;
      final FileInputStream fis = new FileInputStream(sourceZip);
      final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        LOG.debug("Extracting:   " + entry + " from '" + sourceZip.getAbsolutePath() + "'");
        // we need to remove the first element of the path since the
        // folder was compressed but we only want the folders content
        final String entryPath = entry.getName();
        final int indexOf = entryPath.indexOf("/");
        final String cleanUpPath = entryPath.substring(indexOf + 1, entryPath.length());
        final File targetFile = new File(targetFolder, cleanUpPath);
        if (entry.isDirectory()) {
          targetFile.mkdirs();
        } else {
          int count;
          final byte data[] = new byte[BUFFER];
          final FileOutputStream fos = new FileOutputStream(targetFile);
          dest = new BufferedOutputStream(fos, BUFFER);
          while ((count = zis.read(data, 0, BUFFER)) != -1) {
            dest.write(data, 0, count);
          }
          dest.flush();
          dest.close();
        }
      }
      zis.close();
    } catch (final Exception e) {
      throw new RuntimeException("unable to expand upgrade files", e);
    }
  }
}
