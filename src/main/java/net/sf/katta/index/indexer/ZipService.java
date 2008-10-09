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
package net.sf.katta.index.indexer;

import java.io.File;

import net.sf.katta.util.FileUtil;

import org.apache.log4j.Logger;

public class ZipService implements IZipService {

  private final static Logger LOG = Logger.getLogger(ZipService.class);

  public boolean zipFolder(final File inputFolder, final File outputFile) {
    boolean ret = true;
    try {
      FileUtil.zip(inputFolder, outputFile);
    } catch (final Exception e) {
      ret = false;
      LOG.warn("can not create zip file", e);
    }
    return ret;
  }

}