/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.sf.katta.index.indexer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;

public class ZipService implements IZipService {

  private final static Logger LOG = Logger.getLogger(ZipService.class);

  public boolean zipFolder(final File inputFolder, final File outputFile) {
    boolean ret = true;
    try {
      final FileOutputStream fileWriter = new FileOutputStream(outputFile);
      final ZipOutputStream zip = new ZipOutputStream(fileWriter);
      addFolderToZip("", inputFolder, zip);
      zip.flush();
      zip.close();
    } catch (final Exception e) {
      ret = false;
      LOG.warn("can not create zip file", e);
    }
    return ret;
  }

  private void addFolderToZip(final String path, final File folder, final ZipOutputStream zip) throws IOException {
    final String zipEnry = path + (path.equals("") ? "" : File.separator) + folder.getName();
    final File[] listFiles = folder.listFiles();
    for (final File file : listFiles) {
      if (file.isDirectory()) {
        addFolderToZip(zipEnry, file, zip);
      } else {
        addFileToZip(zipEnry, file, zip);
      }
    }
  }

  private void addFileToZip(final String path, final File file, final ZipOutputStream zip) throws IOException {
    final byte[] buffer = new byte[1024];
    int read = -1;
    final FileInputStream in = new FileInputStream(file);
    final String zipEntry = path + File.separator + file.getName();
    LOG.debug("add zip entry: " + zipEntry);
    zip.putNextEntry(new ZipEntry(zipEntry));
    while ((read = in.read(buffer)) > -1) {
      zip.write(buffer, 0, read);
    }
  }

}