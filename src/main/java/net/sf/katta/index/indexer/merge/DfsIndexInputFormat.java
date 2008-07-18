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
package net.sf.katta.index.indexer.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.katta.util.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class DfsIndexInputFormat extends FileInputFormat<Text, DocumentInformation> {

  public static final String DOCUMENT_INFORMATION = "document.duplicate.information.class";

  public RecordReader getRecordReader(final InputSplit inputSplit, final JobConf jobConf, Reporter reporter) throws IOException {
    IDocumentDuplicateInformation duplicateInformation = null;
    String className = jobConf.get(DOCUMENT_INFORMATION);
    try {
      Class<?> byName = jobConf.getClassByName(className);
      duplicateInformation = (IDocumentDuplicateInformation) byName.newInstance();
    } catch (Exception e) {
      Logger.error("can not load class: " + className, e);
      throw new IOException(e.getMessage());
    }
    reporter.setStatus(((FileSplit)inputSplit).getPath().toString());
    return new DfsIndexRecordReader(jobConf, inputSplit, duplicateInformation);
  }

  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    FileSystem fileSystem = FileSystem.get(jobConf);
    Path[] indices = getZipIndices(jobConf, fileSystem);
    InputSplit[] splits = new InputSplit[indices.length];
    for (int i = 0; i < splits.length; i++) {
      splits[i] = new FileSplit(indices[i], 0, Integer.MAX_VALUE, jobConf);
    }
    return splits;
  }

  private Path[] getZipIndices(JobConf jobConf, FileSystem fileSystem) throws IOException {
    Path[] inputPaths = jobConf.getInputPaths();
    List<Path> list = new ArrayList<Path>();
    for (int i = 0; i < inputPaths.length; i++) {
      Path inputPath = inputPaths[i];
      getChilds(fileSystem, list, inputPath);
    }
    return list.toArray(new Path[list.size()]);
  }

  private void getChilds(final FileSystem fileSystem, List<Path> list, Path inputPath) throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(inputPath, new PathFilter() {
      public boolean accept(Path path) {
        boolean ret = false;
        try {
          boolean isFile = fileSystem.isFile(path);
          boolean isZip = path.getName().endsWith(".zip");
          ret = !isFile || isZip;
        } catch (IOException e) {
          Logger.warn("can not get child directory", e);
        }
        return ret;
      }
    });
    for (int j = 0; j < fileStatuses.length; j++) {
      FileStatus fileStatuse = fileStatuses[j];
      Path path = fileStatuse.getPath();
      if (!fileSystem.isFile(path)) {
        getChilds(fileSystem, list, path);
      } else {
        list.add(path);
      }
    }
  }


}
